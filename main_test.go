package main

import (
	"io"
	"io/fs"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
)

func newTestMux(t *testing.T) *http.ServeMux {
	t.Helper()

	dbPath, err := filepath.Abs("game.db")
	if err != nil {
		t.Fatalf("abs path: %v", err)
	}

	schema, err := loadSchema(dbPath)
	if err != nil {
		t.Fatalf("load schema: %v", err)
	}

	a := &app{
		dbPath:      dbPath,
		schema:      schema,
		tableGroups: buildTableGroups(schema),
		lookups:     emptyLookupIndex(),
	}

	staticFS, err := fs.Sub(webFS, "web")
	if err != nil {
		t.Fatalf("sub fs: %v", err)
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/api/overview", a.handleOverview)
	mux.HandleFunc("/api/lookups/", a.handleLookupOptions)
	mux.HandleFunc("/api/users", a.handleUsers)
	mux.HandleFunc("/api/user/", a.handleUserRoutes)
	mux.HandleFunc("/api/table/", a.handleTableRoutes)
	mux.Handle("/", http.FileServer(http.FS(staticFS)))
	return mux
}

func TestOverviewEndpoint(t *testing.T) {
	req := httptest.NewRequest(http.MethodGet, "/api/overview", nil)
	rec := httptest.NewRecorder()

	newTestMux(t).ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}
	if body := rec.Body.String(); body == "" {
		t.Fatal("expected overview body")
	}
}

func TestLookupOptionsEndpoint(t *testing.T) {
	req := httptest.NewRequest(http.MethodGet, "/api/lookups/character_id", nil)
	rec := httptest.NewRecorder()

	newTestMux(t).ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}
	if body := rec.Body.String(); body == "" {
		t.Fatal("expected lookup options body")
	}
}

func TestCharacterLookupOptionsExcludeNonPlayableCharacterIDs(t *testing.T) {
	lookups, err := loadMasterDataLookups("../lunar-tear/server/assets/master_data")
	if err != nil {
		t.Fatalf("load lookups: %v", err)
	}

	options := lookups.lookupOptions("character_id")
	if len(options) == 0 {
		t.Fatal("expected character lookup options")
	}

	foundPlayable := false
	for _, option := range options {
		if option.Value == "1008" && option.Label == "Rion" {
			foundPlayable = true
		}
		if option.Value == "5002504" {
			t.Fatal("expected non-playable character id 5002504 to be excluded from dropdown options")
		}
	}
	if !foundPlayable {
		t.Fatal("expected playable character Rion to appear in dropdown options")
	}
}

func TestStaticIndexServed(t *testing.T) {
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	rec := httptest.NewRecorder()

	newTestMux(t).ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}
	if body := rec.Body.String(); body == "" {
		t.Fatal("expected index body")
	}
}

func TestDeleteUserRemovesUserRow(t *testing.T) {
	source := filepath.Clean("../lunar-tear/server/db/game.db")
	src, err := os.Open(source)
	if err != nil {
		t.Fatalf("open source db: %v", err)
	}
	defer src.Close()

	tmpPath := filepath.Join(t.TempDir(), "game.db")
	dst, err := os.Create(tmpPath)
	if err != nil {
		t.Fatalf("create temp db: %v", err)
	}
	if _, err := io.Copy(dst, src); err != nil {
		dst.Close()
		t.Fatalf("copy db: %v", err)
	}
	if err := dst.Close(); err != nil {
		t.Fatalf("close temp db: %v", err)
	}

	schema, err := loadSchema(tmpPath)
	if err != nil {
		t.Fatalf("load schema: %v", err)
	}

	a := &app{dbPath: tmpPath, schema: schema, lookups: emptyLookupIndex()}
	before := a.mustCount("users")
	if before < 1 {
		t.Skipf("source db has no users in this environment")
	}

	if err := a.deleteUser("1"); err != nil {
		t.Fatalf("delete user: %v", err)
	}

	after := a.mustCount("users")
	if after != before-1 {
		t.Fatalf("expected user count %d after delete, got %d", before-1, after)
	}
}

func TestMasterDataLookupsLoadKnownWeapon(t *testing.T) {
	lookups, err := loadMasterDataLookups("../lunar-tear/server/assets/master_data")
	if err != nil {
		t.Fatalf("load lookups: %v", err)
	}
	if !lookups.summary.Enabled {
		t.Fatal("expected lookups to be enabled")
	}

	entry, ok := lookups.lookupColumn("weapon_id", "101041")
	if !ok {
		t.Fatal("expected weapon_id lookup for 101041")
	}
	if entry.Label == "" {
		t.Fatal("expected populated weapon lookup label")
	}
}

func TestMasterDataLookupsResolveEnglishNames(t *testing.T) {
	lookups, err := loadMasterDataLookups("../lunar-tear/server/assets/master_data")
	if err != nil {
		t.Fatalf("load lookups: %v", err)
	}

	character, ok := lookups.lookupColumn("character_id", "1001")
	if !ok {
		t.Fatal("expected character lookup for 1001")
	}
	if character.Label != "2B" {
		t.Fatalf("expected character 1001 to resolve to 2B, got %q", character.Label)
	}

	weapon, ok := lookups.lookupColumn("weapon_id", "100001")
	if !ok {
		t.Fatal("expected weapon lookup for 100001")
	}
	if weapon.Label != "Faith" {
		t.Fatalf("expected weapon 100001 to resolve to Faith, got %q", weapon.Label)
	}

	darkGun, ok := lookups.lookupColumn("weapon_id", "100011")
	if !ok {
		t.Fatal("expected weapon lookup for 100011")
	}
	if darkGun.Label != "Reincarnation's Shadow" {
		t.Fatalf("expected weapon 100011 to resolve to Reincarnation's Shadow, got %q", darkGun.Label)
	}

	yearning, ok := lookups.lookupColumn("weapon_id", "100021")
	if !ok {
		t.Fatal("expected weapon lookup for 100021")
	}
	if yearning.Label != "Yearning Staff" {
		t.Fatalf("expected weapon 100021 to resolve to Yearning Staff, got %q", yearning.Label)
	}

	anamnesis, ok := lookups.lookupColumn("weapon_id", "101001")
	if !ok {
		t.Fatal("expected weapon lookup for 101001")
	}
	if anamnesis.Label != "Anamnesis of Dawn" {
		t.Fatalf("expected weapon 101001 to resolve to Anamnesis of Dawn, got %q", anamnesis.Label)
	}

	material, ok := lookups.lookupColumn("material_id", "100001")
	if !ok {
		t.Fatal("expected material lookup for 100001")
	}
	if material.Label != "Small Character Enhancement" {
		t.Fatalf("expected material 100001 to resolve to Small Character Enhancement, got %q", material.Label)
	}

	rion, ok := lookups.lookupColumn("character_id", "1008")
	if !ok {
		t.Fatal("expected character lookup for 1008")
	}
	if rion.Label != "Rion" {
		t.Fatalf("expected character 1008 to resolve to Rion, got %q", rion.Label)
	}
}

func TestMasterDataLookupsUseExplicitNierReinEntityColumns(t *testing.T) {
	lookups, err := loadMasterDataLookups("../lunar-tear/server/assets/master_data")
	if err != nil {
		t.Fatalf("load lookups: %v", err)
	}

	for _, tc := range []struct {
		column string
		id     string
	}{
		{column: "character_id", id: "1001"},
		{column: "costume_id", id: "10100"},
		{column: "weapon_id", id: "100001"},
		{column: "companion_id", id: "1"},
		{column: "parts_id", id: "1"},
		{column: "thought_id", id: "10100834"},
		{column: "material_id", id: "100001"},
		{column: "consumable_item_id", id: "1"},
		{column: "important_item_id", id: "100001"},
	} {
		entry, ok := lookups.lookupColumn(tc.column, tc.id)
		if !ok {
			t.Fatalf("expected lookup for %s=%s", tc.column, tc.id)
		}
		if entry.Label == "" {
			t.Fatalf("expected populated label for %s=%s", tc.column, tc.id)
		}
	}
}

func TestMasterDataLookupsSupportFavoriteCostumeAlias(t *testing.T) {
	lookups, err := loadMasterDataLookups("../lunar-tear/server/assets/master_data")
	if err != nil {
		t.Fatalf("load lookups: %v", err)
	}

	direct, ok := lookups.lookupColumn("costume_id", "10100")
	if !ok {
		t.Fatal("expected direct costume lookup for 10100")
	}

	alias, ok := lookups.lookupColumn("favorite_costume_id", "10100")
	if !ok {
		t.Fatal("expected favorite_costume_id alias lookup for 10100")
	}

	if alias.Label != direct.Label {
		t.Fatalf("expected alias label %q to match direct label %q", alias.Label, direct.Label)
	}
}

func TestCostumeLookupFallsBackToCharacterBackedLabel(t *testing.T) {
	lookups, err := loadMasterDataLookups("../lunar-tear/server/assets/master_data")
	if err != nil {
		t.Fatalf("load lookups: %v", err)
	}

	entry, ok := lookups.lookupColumn("costume_id", "10100")
	if !ok {
		t.Fatal("expected costume lookup for 10100")
	}
	if entry.Label != "Rion Costume 10100" {
		t.Fatalf("expected fallback costume label to use character name, got %q", entry.Label)
	}
}

func TestMasterDataLookupsLoadThoughtEntries(t *testing.T) {
	lookups, err := loadMasterDataLookups("../lunar-tear/server/assets/master_data")
	if err != nil {
		t.Fatalf("load lookups: %v", err)
	}

	thought, ok := lookups.lookupColumn("thought_id", "20100112")
	if !ok {
		t.Fatal("expected thought lookup for 20100112")
	}
	if thought.Detail == "" {
		t.Fatal("expected thought lookup detail to be populated")
	}
}

func TestMasterDataLookupsResolveGiftText(t *testing.T) {
	lookups, err := loadMasterDataLookups("../lunar-tear/server/assets/master_data")
	if err != nil {
		t.Fatalf("load lookups: %v", err)
	}

	gift, ok := lookups.lookupColumn("description_gift_text_id", "1")
	if !ok {
		t.Fatal("expected gift text lookup for 1")
	}
	if gift.Label != "Earned from summons." {
		t.Fatalf("expected gift text 1 to resolve to English text, got %q", gift.Label)
	}
}

func TestUserLookupContextResolvesOwnedUUIDs(t *testing.T) {
	lookups, err := loadMasterDataLookups("../lunar-tear/server/assets/master_data")
	if err != nil {
		t.Fatalf("load lookups: %v", err)
	}

	ctx := loadUserLookupContext("../lunar-tear/server/db/game.db", "1", lookups)

	weapon, ok := ctx.lookupColumn("user_weapon_uuid", "73b59e92-bd9f-404d-8a93-07f962047177")
	if !ok {
		t.Fatal("expected owned weapon lookup for test user")
	}
	if weapon.Label != "Faith" {
		t.Fatalf("expected owned weapon UUID to resolve to Faith, got %q", weapon.Label)
	}

	slotAnnotations := ctx.tableAnnotations("user_weapon_skills", map[string]any{
		"user_weapon_uuid": "d8d5a8fd-650b-461a-8fe3-a04504d49907",
		"slot_number":      1,
	})
	if slotAnnotations["slot_number"].Label == "" {
		t.Fatal("expected weapon skill slot to resolve to a skill label")
	}
}
