package main

import (
	"bytes"
	"embed"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"io/fs"
	"log"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"
)

//go:embed web/*
var webFS embed.FS

type app struct {
	dbPath      string
	schema      map[string]tableSchema
	tableGroups []tableGroup
	lookups     *lookupIndex
}

type tableGroup struct {
	Key    string   `json:"key"`
	Label  string   `json:"label"`
	Tables []string `json:"tables"`
}

type tableSchema struct {
	Name       string       `json:"name"`
	Columns    []columnInfo `json:"columns"`
	PrimaryKey []string     `json:"primaryKey"`
}

type columnInfo struct {
	Name       string `json:"name"`
	Type       string `json:"type"`
	NotNull    bool   `json:"notNull"`
	DefaultSQL string `json:"defaultSql"`
	IsPrimary  bool   `json:"isPrimary"`
}

type userSummary struct {
	UserID        int64  `json:"userId"`
	UUID          string `json:"uuid"`
	PlayerID      int64  `json:"playerId"`
	Name          string `json:"name"`
	Message       string `json:"message"`
	Level         int64  `json:"level"`
	Exp           int64  `json:"exp"`
	PaidGem       int64  `json:"paidGem"`
	FreeGem       int64  `json:"freeGem"`
	LatestVersion int64  `json:"latestVersion"`
}

type overviewResponse struct {
	DBPath        string                 `json:"dbPath"`
	UserCount     int64                  `json:"userCount"`
	TableCount    int                    `json:"tableCount"`
	RowCounts     map[string]int64       `json:"rowCounts"`
	Users         []userSummary          `json:"users"`
	Schema        map[string]tableSchema `json:"schema"`
	TableGroups   []tableGroup           `json:"tableGroups"`
	LookupSummary lookupSummary          `json:"lookupSummary"`
}

type tableRowsResponse struct {
	Table       string                   `json:"table"`
	Schema      tableSchema              `json:"schema"`
	Rows        []map[string]any         `json:"rows"`
	Annotations []map[string]lookupEntry `json:"annotations"`
	CanEdit     bool                     `json:"canEdit"`
	Keys        []map[string]string      `json:"keys"`
}

type lookupOption struct {
	Value  string `json:"value"`
	Label  string `json:"label"`
	Detail string `json:"detail,omitempty"`
}

type lookupOptionsResponse struct {
	Column  string         `json:"column"`
	Options []lookupOption `json:"options"`
}

type upsertRequest struct {
	Row map[string]any `json:"row"`
}

type deleteRequest struct {
	Key map[string]any `json:"key"`
}

func main() {
	var (
		dbPath         = flag.String("db", "game.db", "path to the SQLite save database")
		addr           = flag.String("addr", ":8081", "http listen address")
		masterDataPath = flag.String("master-data", "../lunar-tear/server/assets/master_data", "path to the dumped master-data JSON directory")
	)
	flag.Parse()

	absDB, err := filepath.Abs(*dbPath)
	if err != nil {
		log.Fatalf("resolve db path: %v", err)
	}
	wd, err := os.Getwd()
	if err != nil {
		log.Fatalf("resolve working directory: %v", err)
	}
	if _, err := os.Stat(absDB); err != nil {
		log.Fatalf("stat db path: %v", err)
	}
	if _, err := exec.LookPath("sqlite3"); err != nil {
		log.Fatalf("sqlite3 not found in PATH: %v", err)
	}

	schema, err := loadSchema(absDB)
	if err != nil {
		log.Fatalf("load schema: %v", err)
	}

	lookups, err := loadMasterDataLookups(*masterDataPath)
	if err != nil {
		log.Printf("master data lookups unavailable: %v", err)
		lookups = emptyLookupIndex()
	}

	a := &app{
		dbPath:      absDB,
		schema:      schema,
		tableGroups: buildTableGroups(schema),
		lookups:     lookups,
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/api/overview", a.handleOverview)
	mux.HandleFunc("/api/lookups/", a.handleLookupOptions)
	mux.HandleFunc("/api/users", a.handleUsers)
	mux.HandleFunc("/api/user/", a.handleUserRoutes)
	mux.HandleFunc("/api/table/", a.handleTableRoutes)

	staticFS, err := fs.Sub(webFS, "web")
	if err != nil {
		log.Fatalf("prepare static assets: %v", err)
	}
	mux.Handle("/theme/", http.StripPrefix("/theme/", http.FileServer(http.Dir(filepath.Join(wd, "theming")))))
	mux.Handle("/", http.FileServer(http.FS(staticFS)))

	log.Printf("Nier Save Editor listening on http://127.0.0.1%s using %s", *addr, absDB)
	if err := http.ListenAndServe(*addr, logRequest(mux)); err != nil {
		log.Fatalf("serve: %v", err)
	}
}

func logRequest(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		log.Printf("%s %s", r.Method, r.URL.Path)
		next.ServeHTTP(w, r)
	})
}

func (a *app) handleOverview(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	counts := map[string]int64{}
	for _, table := range []string{
		"users", "user_profile", "user_status", "user_gem", "user_materials", "user_consumable_items",
		"user_characters", "user_costumes", "user_weapons", "user_quests", "user_missions",
	} {
		counts[table] = a.mustCount(table)
	}

	resp := overviewResponse{
		DBPath:        a.dbPath,
		UserCount:     a.mustCount("users"),
		TableCount:    len(a.schema),
		RowCounts:     counts,
		Users:         a.listUsers(),
		Schema:        a.schema,
		TableGroups:   a.tableGroups,
		LookupSummary: a.lookups.summary,
	}
	writeJSON(w, resp)
}

func (a *app) handleUsers(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	writeJSON(w, a.listUsers())
}

func (a *app) handleLookupOptions(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	column := strings.TrimPrefix(r.URL.Path, "/api/lookups/")
	if column == "" || strings.Contains(column, "/") {
		http.NotFound(w, r)
		return
	}

	writeJSON(w, lookupOptionsResponse{
		Column:  column,
		Options: a.lookups.lookupOptions(column),
	})
}

func (a *app) handleUserRoutes(w http.ResponseWriter, r *http.Request) {
	path := strings.TrimPrefix(r.URL.Path, "/api/user/")
	parts := strings.Split(path, "/")
	if len(parts) == 1 && r.Method == http.MethodDelete {
		a.handleDeleteUser(w, parts[0])
		return
	}
	if len(parts) < 2 {
		http.NotFound(w, r)
		return
	}

	userID := parts[0]
	switch {
	case len(parts) == 2 && parts[1] == "summary" && r.Method == http.MethodGet:
		a.handleUserSummary(w, userID)
	case len(parts) == 3 && parts[1] == "table" && r.Method == http.MethodGet:
		a.handleTableRows(w, parts[2], userID)
	case len(parts) == 3 && parts[1] == "table" && r.Method == http.MethodPost:
		a.handleUpsertRow(w, r, parts[2], userID)
	case len(parts) == 3 && parts[1] == "table" && r.Method == http.MethodDelete:
		a.handleDeleteRow(w, r, parts[2], userID)
	default:
		http.NotFound(w, r)
	}
}

func (a *app) handleDeleteUser(w http.ResponseWriter, userID string) {
	if strings.TrimSpace(userID) == "" {
		http.Error(w, "user id is required", http.StatusBadRequest)
		return
	}

	if err := a.deleteUser(userID); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	writeJSON(w, map[string]any{"ok": true})
}

func (a *app) handleTableRoutes(w http.ResponseWriter, r *http.Request) {
	table := strings.TrimPrefix(r.URL.Path, "/api/table/")
	if table == "" || strings.Contains(table, "/") {
		http.NotFound(w, r)
		return
	}

	userID := r.URL.Query().Get("user_id")
	switch r.Method {
	case http.MethodGet:
		a.handleTableRows(w, table, userID)
	case http.MethodPost:
		a.handleUpsertRow(w, r, table, userID)
	case http.MethodDelete:
		a.handleDeleteRow(w, r, table, userID)
	default:
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	}
}

func (a *app) handleUserSummary(w http.ResponseWriter, userID string) {
	rows, err := querySQLiteJSON(a.dbPath, fmt.Sprintf(`
		SELECT u.user_id, u.uuid, u.player_id, COALESCE(p.name, '') AS name, COALESCE(p.message, '') AS message,
		       COALESCE(s.level, 0) AS level, COALESCE(s.exp, 0) AS exp,
		       COALESCE(g.paid_gem, 0) AS paid_gem, COALESCE(g.free_gem, 0) AS free_gem,
		       COALESCE(u.latest_version, 0) AS latest_version
		FROM users u
		LEFT JOIN user_profile p ON p.user_id = u.user_id
		LEFT JOIN user_status s ON s.user_id = u.user_id
		LEFT JOIN user_gem g ON g.user_id = u.user_id
		WHERE u.user_id = %s
	`, sqlLiteral(columnInfo{Type: "INTEGER"}, userID)))
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	if len(rows) == 0 {
		http.Error(w, "user not found", http.StatusNotFound)
		return
	}

	row := rows[0]
	writeJSON(w, userSummary{
		UserID:        toInt64(row["user_id"]),
		UUID:          stringifyValue(row["uuid"]),
		PlayerID:      toInt64(row["player_id"]),
		Name:          stringifyValue(row["name"]),
		Message:       stringifyValue(row["message"]),
		Level:         toInt64(row["level"]),
		Exp:           toInt64(row["exp"]),
		PaidGem:       toInt64(row["paid_gem"]),
		FreeGem:       toInt64(row["free_gem"]),
		LatestVersion: toInt64(row["latest_version"]),
	})
}

func (a *app) handleTableRows(w http.ResponseWriter, table, userID string) {
	schema, ok := a.schema[table]
	if !ok {
		http.Error(w, "unsupported table", http.StatusBadRequest)
		return
	}

	query := fmt.Sprintf("SELECT * FROM %s", table)
	if userID != "" && hasColumn(schema, "user_id") {
		query += fmt.Sprintf(" WHERE user_id = %s", sqlLiteral(columnByName(schema, "user_id"), userID))
	}
	query += " ORDER BY ROWID"

	rows, err := querySQLiteJSON(a.dbPath, query)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	userLookups := loadUserLookupContext(a.dbPath, userID, a.lookups)

	keys := make([]map[string]string, 0, len(rows))
	annotations := make([]map[string]lookupEntry, 0, len(rows))
	for _, row := range rows {
		key := map[string]string{}
		for _, col := range schema.PrimaryKey {
			key[col] = stringifyValue(row[col])
		}
		keys = append(keys, key)
		annotations = append(annotations, a.lookups.annotateRow(table, row, userLookups))
	}

	writeJSON(w, tableRowsResponse{
		Table:       table,
		Schema:      schema,
		Rows:        rows,
		Annotations: annotations,
		CanEdit:     len(schema.PrimaryKey) > 0,
		Keys:        keys,
	})
}

func (a *app) handleUpsertRow(w http.ResponseWriter, r *http.Request, table, userID string) {
	schema, ok := a.schema[table]
	if !ok || len(schema.PrimaryKey) == 0 {
		http.Error(w, "unsupported table", http.StatusBadRequest)
		return
	}

	var req upsertRequest
	if err := decodeJSON(r.Body, &req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if req.Row == nil {
		http.Error(w, "row is required", http.StatusBadRequest)
		return
	}
	if userID != "" && hasColumn(schema, "user_id") {
		req.Row["user_id"] = userID
	}

	query, err := buildUpsert(table, schema, req.Row)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if err := execSQLite(a.dbPath, query); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	writeJSON(w, map[string]any{"ok": true})
}

func (a *app) handleDeleteRow(w http.ResponseWriter, r *http.Request, table, userID string) {
	schema, ok := a.schema[table]
	if !ok || len(schema.PrimaryKey) == 0 {
		http.Error(w, "unsupported table", http.StatusBadRequest)
		return
	}

	var req deleteRequest
	if err := decodeJSON(r.Body, &req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if req.Key == nil {
		req.Key = map[string]any{}
	}
	if userID != "" && hasColumn(schema, "user_id") {
		req.Key["user_id"] = userID
	}

	query, err := buildDelete(table, schema, req.Key)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if err := execSQLite(a.dbPath, query); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	writeJSON(w, map[string]any{"ok": true})
}

func (a *app) listUsers() []userSummary {
	rows, err := querySQLiteJSON(a.dbPath, `
		SELECT u.user_id, u.uuid, u.player_id, COALESCE(p.name, '') AS name, COALESCE(p.message, '') AS message,
		       COALESCE(s.level, 0) AS level, COALESCE(s.exp, 0) AS exp,
		       COALESCE(g.paid_gem, 0) AS paid_gem, COALESCE(g.free_gem, 0) AS free_gem,
		       COALESCE(u.latest_version, 0) AS latest_version
		FROM users u
		LEFT JOIN user_profile p ON p.user_id = u.user_id
		LEFT JOIN user_status s ON s.user_id = u.user_id
		LEFT JOIN user_gem g ON g.user_id = u.user_id
		ORDER BY u.user_id
	`)
	if err != nil {
		log.Printf("list users: %v", err)
		return nil
	}

	result := make([]userSummary, 0, len(rows))
	for _, row := range rows {
		result = append(result, userSummary{
			UserID:        toInt64(row["user_id"]),
			UUID:          stringifyValue(row["uuid"]),
			PlayerID:      toInt64(row["player_id"]),
			Name:          stringifyValue(row["name"]),
			Message:       stringifyValue(row["message"]),
			Level:         toInt64(row["level"]),
			Exp:           toInt64(row["exp"]),
			PaidGem:       toInt64(row["paid_gem"]),
			FreeGem:       toInt64(row["free_gem"]),
			LatestVersion: toInt64(row["latest_version"]),
		})
	}
	return result
}

func loadSchema(dbPath string) (map[string]tableSchema, error) {
	tables, err := querySQLiteJSON(dbPath, `
		SELECT name
		FROM sqlite_master
		WHERE type = 'table' AND name NOT LIKE 'sqlite_%'
		ORDER BY name
	`)
	if err != nil {
		return nil, err
	}

	schema := map[string]tableSchema{}
	for _, item := range tables {
		name := stringifyValue(item["name"])
		pragmaRows, err := querySQLiteJSON(dbPath, fmt.Sprintf("PRAGMA table_info(%s)", name))
		if err != nil {
			return nil, err
		}

		var cols []columnInfo
		var pkPairs []struct {
			pos  int
			name string
		}
		for _, row := range pragmaRows {
			col := columnInfo{
				Name:       stringifyValue(row["name"]),
				Type:       stringifyValue(row["type"]),
				NotNull:    toInt64(row["notnull"]) != 0,
				DefaultSQL: stringifyValue(row["dflt_value"]),
				IsPrimary:  toInt64(row["pk"]) > 0,
			}
			cols = append(cols, col)
			if col.IsPrimary {
				pkPairs = append(pkPairs, struct {
					pos  int
					name string
				}{pos: int(toInt64(row["pk"])), name: col.Name})
			}
		}

		sort.Slice(pkPairs, func(i, j int) bool { return pkPairs[i].pos < pkPairs[j].pos })
		primaryKey := make([]string, 0, len(pkPairs))
		for _, pair := range pkPairs {
			primaryKey = append(primaryKey, pair.name)
		}

		schema[name] = tableSchema{Name: name, Columns: cols, PrimaryKey: primaryKey}
	}

	return schema, nil
}

func buildUpsert(table string, schema tableSchema, row map[string]any) (string, error) {
	var (
		columns       []string
		values        []string
		updateClauses []string
	)

	for _, col := range schema.Columns {
		if value, ok := row[col.Name]; ok {
			columns = append(columns, col.Name)
			values = append(values, sqlLiteral(col, value))
			if !contains(schema.PrimaryKey, col.Name) {
				updateClauses = append(updateClauses, fmt.Sprintf("%s = excluded.%s", col.Name, col.Name))
			}
		}
	}
	if len(columns) == 0 {
		return "", errors.New("no known columns in row payload")
	}

	query := fmt.Sprintf(
		"INSERT INTO %s (%s) VALUES (%s)",
		table,
		strings.Join(columns, ", "),
		strings.Join(values, ", "),
	)
	if len(updateClauses) > 0 {
		query += fmt.Sprintf(" ON CONFLICT (%s) DO UPDATE SET %s", strings.Join(schema.PrimaryKey, ", "), strings.Join(updateClauses, ", "))
	} else {
		query += fmt.Sprintf(" ON CONFLICT (%s) DO NOTHING", strings.Join(schema.PrimaryKey, ", "))
	}
	return query, nil
}

func buildDelete(table string, schema tableSchema, key map[string]any) (string, error) {
	var parts []string
	for _, col := range schema.PrimaryKey {
		value, ok := key[col]
		if !ok {
			return "", fmt.Errorf("missing primary key column %q", col)
		}
		parts = append(parts, fmt.Sprintf("%s = %s", col, sqlLiteral(columnByName(schema, col), value)))
	}
	return fmt.Sprintf("DELETE FROM %s WHERE %s", table, strings.Join(parts, " AND ")), nil
}

func querySQLiteJSON(dbPath, query string) ([]map[string]any, error) {
	stdout, _, err := runSQLite(dbPath, "-json", query)
	if err != nil {
		return nil, err
	}
	if strings.TrimSpace(stdout) == "" {
		return []map[string]any{}, nil
	}

	var rows []map[string]any
	if err := json.Unmarshal([]byte(stdout), &rows); err != nil {
		return nil, err
	}
	return rows, nil
}

func execSQLite(dbPath, query string) error {
	_, _, err := runSQLite(dbPath, query)
	return err
}

func runSQLite(dbPath string, args ...string) (string, string, error) {
	baseArgs := []string{"-cmd", ".timeout 5000", dbPath}
	baseArgs = append(baseArgs, args...)

	var lastErr error
	for attempt := 0; attempt < 4; attempt++ {
		cmd := exec.Command("sqlite3", baseArgs...)
		var stdout, stderr bytes.Buffer
		cmd.Stdout = &stdout
		cmd.Stderr = &stderr
		err := cmd.Run()
		if err == nil {
			return stdout.String(), stderr.String(), nil
		}

		errText := strings.TrimSpace(stderr.String())
		lastErr = fmt.Errorf("sqlite3 failed: %s", errText)
		if !isSQLiteBusy(errText) {
			return "", errText, lastErr
		}

		time.Sleep(time.Duration(attempt+1) * 350 * time.Millisecond)
	}

	return "", "", lastErr
}

func isSQLiteBusy(errText string) bool {
	errText = strings.ToLower(errText)
	return strings.Contains(errText, "database is locked") ||
		strings.Contains(errText, "database table is locked") ||
		strings.Contains(errText, "database schema is locked") ||
		strings.Contains(errText, "busy")
}

func sqlLiteral(col columnInfo, value any) string {
	if value == nil {
		return "NULL"
	}

	colType := strings.ToUpper(col.Type)
	if strings.Contains(colType, "INT") || strings.Contains(colType, "REAL") || strings.Contains(colType, "FLOA") || strings.Contains(colType, "DOUB") {
		switch v := value.(type) {
		case string:
			if strings.TrimSpace(v) == "" {
				return "NULL"
			}
			return strings.TrimSpace(v)
		default:
			return fmt.Sprint(v)
		}
	}

	text := stringifyValue(value)
	text = strings.ReplaceAll(text, "'", "''")
	return "'" + text + "'"
}

func writeJSON(w http.ResponseWriter, value any) {
	w.Header().Set("Content-Type", "application/json")
	encoder := json.NewEncoder(w)
	encoder.SetIndent("", "  ")
	_ = encoder.Encode(value)
}

func decodeJSON(r io.Reader, dest any) error {
	decoder := json.NewDecoder(r)
	decoder.DisallowUnknownFields()
	return decoder.Decode(dest)
}

func (a *app) mustCount(table string) int64 {
	rows, err := querySQLiteJSON(a.dbPath, fmt.Sprintf("SELECT COUNT(*) AS count FROM %s", table))
	if err != nil || len(rows) == 0 {
		return 0
	}
	return toInt64(rows[0]["count"])
}

func columnByName(schema tableSchema, name string) columnInfo {
	for _, col := range schema.Columns {
		if col.Name == name {
			return col
		}
	}
	return columnInfo{Name: name, Type: "TEXT"}
}

func hasColumn(schema tableSchema, name string) bool {
	for _, col := range schema.Columns {
		if col.Name == name {
			return true
		}
	}
	return false
}

func buildTableGroups(schema map[string]tableSchema) []tableGroup {
	grouped := map[string][]string{}
	for name, table := range schema {
		switch {
		case strings.HasPrefix(name, "sqlite_"):
			continue
		}
		key, label := classifyTable(name, table)
		grouped[key+"|"+label] = append(grouped[key+"|"+label], name)
	}

	var groups []tableGroup
	for keyLabel, tables := range grouped {
		sort.Strings(tables)
		parts := strings.SplitN(keyLabel, "|", 2)
		groups = append(groups, tableGroup{
			Key:    parts[0],
			Label:  parts[1],
			Tables: tables,
		})
	}
	order := map[string]int{
		"identity":   0,
		"profile":    1,
		"inventory":  2,
		"economy":    3,
		"collection": 4,
		"deck":       5,
		"progress":   6,
		"combat":     7,
		"user-misc":  8,
		"global":     9,
		"system":     10,
	}
	sort.Slice(groups, func(i, j int) bool {
		oi, iok := order[groups[i].Key]
		oj, jok := order[groups[j].Key]
		if iok && jok && oi != oj {
			return oi < oj
		}
		return groups[i].Label < groups[j].Label
	})
	return groups
}

func classifyTable(name string, schema tableSchema) (string, string) {
	switch {
	case name == "goose_db_version":
		return "system", "System"
	case name == "users" || name == "sessions":
		return "identity", "Identity"
	case name == "user_profile" || name == "user_status" || name == "user_setting" || name == "user_login" || name == "user_login_bonus":
		return "profile", "Profile and Account"
	case name == "user_gem" || strings.Contains(name, "gacha") || strings.Contains(name, "shop") || strings.Contains(name, "gift"):
		return "economy", "Economy, Shop, and Gacha"
	case strings.Contains(name, "weapon") || strings.Contains(name, "costume") || strings.Contains(name, "character") || strings.Contains(name, "companion") || strings.Contains(name, "thought") || strings.Contains(name, "parts"):
		return "collection", "Collection and Loadout"
	case strings.Contains(name, "deck"):
		return "deck", "Decks and Party Setup"
	case strings.Contains(name, "quest") || strings.Contains(name, "mission") || strings.Contains(name, "story") || strings.Contains(name, "tutorial") || strings.Contains(name, "explore") || strings.Contains(name, "portal") || strings.Contains(name, "cage"):
		return "progress", "Progress and World State"
	case strings.Contains(name, "battle") || strings.Contains(name, "hunt") || strings.Contains(name, "gimmick"):
		return "combat", "Combat and Challenge State"
	case strings.Contains(name, "material") || strings.Contains(name, "consumable") || strings.Contains(name, "important_item") || strings.Contains(name, "premium_item"):
		return "inventory", "Inventory and Currency"
	case hasColumn(schema, "user_id"):
		return "user-misc", "Other User Tables"
	default:
		return "global", "Other Global Tables"
	}
}

func (a *app) deleteUser(userID string) error {
	if _, err := strconv.ParseInt(strings.TrimSpace(userID), 10, 64); err != nil {
		return fmt.Errorf("invalid user id %q", userID)
	}

	var childDeletes []string
	for name, schema := range a.schema {
		if name == "users" || strings.HasPrefix(name, "sqlite_") {
			continue
		}
		if hasColumn(schema, "user_id") {
			childDeletes = append(childDeletes, fmt.Sprintf(
				"DELETE FROM %s WHERE user_id = %s",
				name,
				sqlLiteral(columnByName(schema, "user_id"), userID),
			))
		}
	}
	sort.Strings(childDeletes)

	statements := append([]string{"BEGIN IMMEDIATE"}, childDeletes...)
	statements = append(statements, fmt.Sprintf(
		"DELETE FROM users WHERE user_id = %s",
		sqlLiteral(columnInfo{Type: "INTEGER"}, userID),
	), "COMMIT")

	query := strings.Join(statements, ";\n") + ";"
	if err := execSQLite(a.dbPath, query); err != nil {
		_ = execSQLite(a.dbPath, "ROLLBACK;")
		return err
	}
	return nil
}

func contains(values []string, target string) bool {
	for _, value := range values {
		if value == target {
			return true
		}
	}
	return false
}

func stringifyValue(v any) string {
	switch value := v.(type) {
	case nil:
		return ""
	case string:
		return value
	case float64:
		if value == float64(int64(value)) {
			return strconv.FormatInt(int64(value), 10)
		}
		return strconv.FormatFloat(value, 'f', -1, 64)
	case bool:
		if value {
			return "1"
		}
		return "0"
	default:
		return fmt.Sprint(value)
	}
}

func toInt64(v any) int64 {
	switch value := v.(type) {
	case nil:
		return 0
	case float64:
		return int64(value)
	case int64:
		return value
	case string:
		parsed, _ := strconv.ParseInt(strings.TrimSpace(value), 10, 64)
		return parsed
	default:
		parsed, _ := strconv.ParseInt(fmt.Sprint(value), 10, 64)
		return parsed
	}
}

func init() {
	log.SetFlags(0)
	log.SetOutput(os.Stdout)
}
