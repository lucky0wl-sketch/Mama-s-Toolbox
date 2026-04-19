package main

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"
)

type lookupSummary struct {
	Enabled    bool     `json:"enabled"`
	SourcePath string   `json:"sourcePath"`
	EntryCount int      `json:"entryCount"`
	Kinds      []string `json:"kinds"`
}

type lookupEntry struct {
	Label  string `json:"label"`
	Detail string `json:"detail,omitempty"`
}

type lookupIndex struct {
	summary             lookupSummary
	columns             map[string]map[string]lookupEntry
	playableCharacterIDs map[string]struct{}
	weaponAbilitySlots  map[string]map[int]lookupEntry
	weaponSkillSlots    map[string]map[int]lookupEntry
	costumeActiveSkills map[string]map[int]lookupEntry
}

type textBundleLocalizer struct {
	bundles map[string]map[string]string
}

type userLookupContext struct {
	columns           map[string]map[string]lookupEntry
	weaponRefs        map[string]ownedEntityRef
	costumeRefs       map[string]ownedEntityRef
	companionRefs     map[string]ownedEntityRef
	partsRefs         map[string]ownedEntityRef
	thoughtRefs       map[string]ownedEntityRef
	deckCharacterRefs map[string]lookupEntry
}

type ownedEntityRef struct {
	entry        lookupEntry
	entityID     string
	limitBreak   int
	abilitySlots map[int]lookupEntry
	skillSlots   map[int]lookupEntry
	activeSkill  lookupEntry
}

type nierReinLookupContext struct {
	masterDataPath      string
	textLocalizer       *textBundleLocalizer
	weaponEvolution     map[string]int
	partsGroupAsset     map[string]string
	characterNames      map[string]string
	thoughtCatalogTerms map[string]int64
}

var nierReinEntityColumns = map[string]struct{}{
	"character_id":       {},
	"costume_id":         {},
	"weapon_id":          {},
	"companion_id":       {},
	"parts_id":           {},
	"thought_id":         {},
	"material_id":        {},
	"consumable_item_id": {},
	"important_item_id":  {},
}

var nierReinLookupAliases = map[string]string{
	"favorite_costume_id": "costume_id",
}

func emptyLookupIndex() *lookupIndex {
	return &lookupIndex{
		summary:             lookupSummary{},
		columns:             map[string]map[string]lookupEntry{},
		playableCharacterIDs: map[string]struct{}{},
		weaponAbilitySlots:  map[string]map[int]lookupEntry{},
		weaponSkillSlots:    map[string]map[int]lookupEntry{},
		costumeActiveSkills: map[string]map[int]lookupEntry{},
	}
}

func loadMasterDataLookups(masterDataPath string) (*lookupIndex, error) {
	if strings.TrimSpace(masterDataPath) == "" {
		return emptyLookupIndex(), nil
	}

	absPath, err := filepath.Abs(masterDataPath)
	if err != nil {
		return nil, fmt.Errorf("resolve master data path: %w", err)
	}
	if _, err := os.Stat(absPath); err != nil {
		return nil, fmt.Errorf("stat master data path: %w", err)
	}

	index := &lookupIndex{
		summary: lookupSummary{
			Enabled:    true,
			SourcePath: absPath,
		},
		columns:             map[string]map[string]lookupEntry{},
		playableCharacterIDs: map[string]struct{}{},
		weaponAbilitySlots:  map[string]map[int]lookupEntry{},
		weaponSkillSlots:    map[string]map[int]lookupEntry{},
		costumeActiveSkills: map[string]map[int]lookupEntry{},
	}

	ctx := newNierReinLookupContext(absPath)
	if err := index.loadCharacterLookupsFromNierRein(ctx); err != nil {
		return nil, err
	}
	if err := index.loadCostumeLookupsFromNierRein(ctx); err != nil {
		return nil, err
	}
	if err := index.loadWeaponLookupsFromNierRein(ctx); err != nil {
		return nil, err
	}
	if err := index.loadCompanionLookupsFromNierRein(ctx); err != nil {
		return nil, err
	}
	if err := index.loadConsumableLookupsFromNierRein(ctx); err != nil {
		return nil, err
	}
	if err := index.loadMaterialLookupsFromNierRein(ctx); err != nil {
		return nil, err
	}
	if err := index.loadImportantItemLookupsFromNierRein(ctx); err != nil {
		return nil, err
	}
	if err := index.loadPremiumItemLookupsFromNierRein(ctx); err != nil {
		return nil, err
	}
	if err := index.loadPartsLookupsFromNierRein(ctx); err != nil {
		return nil, err
	}
	if err := index.loadThoughtLookupsFromNierRein(ctx); err != nil {
		return nil, err
	}

	if err := index.loadAbilityLookups(ctx.masterDataPath, ctx.textLocalizer); err != nil {
		return nil, err
	}
	if err := index.loadSkillLookups(ctx.masterDataPath, ctx.textLocalizer); err != nil {
		return nil, err
	}
	if err := index.loadGiftTextLookups(ctx.masterDataPath); err != nil {
		return nil, err
	}
	if err := index.loadShopItemLookups(ctx.masterDataPath, ctx.textLocalizer); err != nil {
		return nil, err
	}
	if err := index.loadCharacterBoardLookups(ctx.masterDataPath, ctx.characterNames); err != nil {
		return nil, err
	}
	if err := index.loadWeaponSlotLookups(ctx.masterDataPath); err != nil {
		return nil, err
	}
	if err := index.loadCostumeActiveSkillLookups(ctx.masterDataPath); err != nil {
		return nil, err
	}

	for column, entries := range index.columns {
		if len(entries) == 0 {
			continue
		}
		index.summary.Kinds = append(index.summary.Kinds, column)
		index.summary.EntryCount += len(entries)
	}
	sort.Strings(index.summary.Kinds)

	return index, nil
}

func canonicalNierReinLookupColumn(column string) string {
	if canonical, ok := nierReinLookupAliases[column]; ok {
		return canonical
	}
	if _, ok := nierReinEntityColumns[column]; ok {
		return column
	}
	return ""
}

func newNierReinLookupContext(masterDataPath string) *nierReinLookupContext {
	assetsRoot := filepath.Dir(masterDataPath)
	textLocalizer := loadTextBundleLocalizer(assetsRoot)
	return &nierReinLookupContext{
		masterDataPath:      masterDataPath,
		textLocalizer:       textLocalizer,
		weaponEvolution:     loadWeaponEvolutionOrders(masterDataPath),
		partsGroupAsset:     loadPartsGroupAssets(masterDataPath),
		characterNames:      loadCharacterNames(masterDataPath, textLocalizer),
		thoughtCatalogTerms: loadThoughtCatalogTerms(masterDataPath),
	}
}

func (l *lookupIndex) loadCharacterLookupsFromNierRein(ctx *nierReinLookupContext) error {
	rows, err := readJSONArray(filepath.Join(ctx.masterDataPath, "EntityMCharacterTable.json"))
	if err != nil {
		return fmt.Errorf("load EntityMCharacterTable.json: %w", err)
	}
	if l.columns["character_id"] == nil {
		l.columns["character_id"] = map[string]lookupEntry{}
	}
	for _, row := range rows {
		characterID := stringifyValue(row["CharacterId"])
		if characterID == "" {
			continue
		}
		label := fmt.Sprintf("Character %s", characterID)
		if ctx.textLocalizer != nil {
			if name := ctx.textLocalizer.characterName(toInt64(row["NameCharacterTextId"])); name != "" {
				label = name
			}
		}
		l.columns["character_id"][characterID] = lookupEntry{
			Label: label,
			Detail: joinDetail(
				idPair("default costume", row["DefaultCostumeId"]),
				idPair("default weapon", row["DefaultWeaponId"]),
			),
		}
		if isPlayableCharacterRow(row) {
			l.playableCharacterIDs[characterID] = struct{}{}
		}
	}
	return nil
}

func isPlayableCharacterRow(row map[string]any) bool {
	return toInt64(row["DefaultCostumeId"]) > 0 || toInt64(row["DefaultWeaponId"]) > 0 || toInt64(row["EndCostumeId"]) > 0 || toInt64(row["EndWeaponId"]) > 0
}

func (l *lookupIndex) lookupOptions(column string) []lookupOption {
	if l == nil {
		return nil
	}
	canonical := canonicalNierReinLookupColumn(column)
	if canonical == "" {
		canonical = column
	}
	entries, ok := l.columns[canonical]
	if !ok || len(entries) == 0 {
		return nil
	}
	options := make([]lookupOption, 0, len(entries))
	for value, entry := range entries {
		if canonical == "character_id" && len(l.playableCharacterIDs) > 0 {
			if _, ok := l.playableCharacterIDs[value]; !ok {
				continue
			}
		}
		options = append(options, lookupOption{
			Value:  value,
			Label:  entry.Label,
			Detail: entry.Detail,
		})
	}
	sort.Slice(options, func(i, j int) bool {
		left := strings.ToLower(options[i].Label)
		right := strings.ToLower(options[j].Label)
		if left == right {
			return options[i].Value < options[j].Value
		}
		return left < right
	})
	return options
}

func (l *lookupIndex) loadCostumeLookupsFromNierRein(ctx *nierReinLookupContext) error {
	return l.loadFile(ctx.masterDataPath, "EntityMCostumeTable.json", "CostumeId", "costume_id",
		func(row map[string]any) string {
			if ctx.textLocalizer != nil {
				if name := ctx.textLocalizer.costumeName(costumeActorAssetID(row)); name != "" {
					return name
				}
			}
			if name := ctx.characterNames[stringifyValue(row["CharacterId"])]; name != "" {
				return fmt.Sprintf("%s Costume %s", name, stringifyValue(row["CostumeId"]))
			}
			return fmt.Sprintf("Costume %s", stringifyValue(row["CostumeId"]))
		},
		func(row map[string]any) string {
			characterID := stringifyValue(row["CharacterId"])
			characterDetail := "character " + characterID
			if name := ctx.characterNames[characterID]; name != "" {
				characterDetail = fmt.Sprintf("%s (%s)", name, characterDetail)
			}
			return joinDetail(
				characterDetail,
			)
		},
	)
}

func (l *lookupIndex) loadWeaponLookupsFromNierRein(ctx *nierReinLookupContext) error {
	return l.loadFile(ctx.masterDataPath, "EntityMWeaponTable.json", "WeaponId", "weapon_id",
		func(row map[string]any) string {
			if ctx.textLocalizer != nil {
				weaponID := stringifyValue(row["WeaponId"])
				for _, assetID := range weaponNameAssetIDs(row) {
					if name := ctx.textLocalizer.weaponName(assetID, ctx.weaponEvolution[weaponID]); name != "" {
						return name
					}
				}
			}
			return fmt.Sprintf("Weapon %s", stringifyValue(row["WeaponId"]))
		},
		func(row map[string]any) string {
			return joinDetail(
				idPair("type", row["WeaponType"]),
				idPair("attribute", row["AttributeType"]),
			)
		},
	)
}

func (l *lookupIndex) loadCompanionLookupsFromNierRein(ctx *nierReinLookupContext) error {
	return l.loadFile(ctx.masterDataPath, "EntityMCompanionTable.json", "CompanionId", "companion_id",
		func(row map[string]any) string {
			if ctx.textLocalizer != nil {
				if name := ctx.textLocalizer.companionName(companionActorAssetID(row)); name != "" {
					return name
				}
			}
			return fmt.Sprintf("Companion %s", stringifyValue(row["CompanionId"]))
		},
		func(row map[string]any) string {
			return joinDetail(
				idPair("category", row["CompanionCategoryType"]),
				idPair("attribute", row["AttributeType"]),
				idPair("skill", row["SkillId"]),
			)
		},
	)
}

func (l *lookupIndex) loadConsumableLookupsFromNierRein(ctx *nierReinLookupContext) error {
	return l.loadFile(ctx.masterDataPath, "EntityMConsumableItemTable.json", "ConsumableItemId", "consumable_item_id",
		func(row map[string]any) string {
			if ctx.textLocalizer != nil {
				if name := ctx.textLocalizer.consumableName(toInt64(row["AssetCategoryId"]), toInt64(row["AssetVariationId"])); name != "" {
					return name
				}
			}
			return fmt.Sprintf("Consumable %s", stringifyValue(row["ConsumableItemId"]))
		},
		func(row map[string]any) string {
			return joinDetail(
				idPair("type", row["ConsumableItemType"]),
				idPair("term", row["ConsumableItemTermId"]),
			)
		},
	)
}

func (l *lookupIndex) loadMaterialLookupsFromNierRein(ctx *nierReinLookupContext) error {
	return l.loadFile(ctx.masterDataPath, "EntityMMaterialTable.json", "MaterialId", "material_id",
		func(row map[string]any) string {
			if ctx.textLocalizer != nil {
				if name := ctx.textLocalizer.materialName(toInt64(row["AssetCategoryId"]), toInt64(row["AssetVariationId"])); name != "" {
					return name
				}
			}
			return fmt.Sprintf("Material %s", stringifyValue(row["MaterialId"]))
		},
		func(row map[string]any) string {
			return joinDetail(
				idPair("type", row["MaterialType"]),
				idPair("attribute", row["AttributeType"]),
			)
		},
	)
}

func (l *lookupIndex) loadImportantItemLookupsFromNierRein(ctx *nierReinLookupContext) error {
	return l.loadFile(ctx.masterDataPath, "EntityMImportantItemTable.json", "ImportantItemId", "important_item_id",
		func(row map[string]any) string {
			if ctx.textLocalizer != nil {
				if name := ctx.textLocalizer.importantItemName(toInt64(row["NameImportantItemTextId"])); name != "" {
					return name
				}
			}
			return fmt.Sprintf("Important Item %s", stringifyValue(row["ImportantItemId"]))
		},
		func(row map[string]any) string {
			return joinDetail(
				textPair("name text", row["NameImportantItemTextId"]),
				idPair("type", row["ImportantItemType"]),
			)
		},
	)
}

func (l *lookupIndex) loadPremiumItemLookupsFromNierRein(ctx *nierReinLookupContext) error {
	return l.loadFile(ctx.masterDataPath, "EntityMPremiumItemTable.json", "PremiumItemId", "premium_item_id",
		func(row map[string]any) string {
			return fmt.Sprintf("Premium Item %s", stringifyValue(row["PremiumItemId"]))
		},
		func(row map[string]any) string {
			return joinDetail(
				idPair("type", row["PremiumItemType"]),
				dateRange(row["StartDatetime"], row["EndDatetime"]),
			)
		},
	)
}

func (l *lookupIndex) loadPartsLookupsFromNierRein(ctx *nierReinLookupContext) error {
	return l.loadFile(ctx.masterDataPath, "EntityMPartsTable.json", "PartsId", "parts_id",
		func(row map[string]any) string {
			if ctx.textLocalizer != nil {
				groupID := stringifyValue(row["PartsGroupId"])
				if assetID := ctx.partsGroupAsset[groupID]; assetID != "" {
					if name := ctx.textLocalizer.partsGroupName(assetID); name != "" {
						return name
					}
				}
			}
			return fmt.Sprintf("Part %s", stringifyValue(row["PartsId"]))
		},
		func(row map[string]any) string {
			return joinDetail(
				idPair("group", row["PartsGroupId"]),
				idPair("initial lottery", row["PartsInitialLotteryId"]),
			)
		},
	)
}

func (l *lookupIndex) loadThoughtLookupsFromNierRein(ctx *nierReinLookupContext) error {
	return l.loadFile(ctx.masterDataPath, "EntityMThoughtTable.json", "ThoughtId", "thought_id",
		func(row map[string]any) string {
			if ctx.textLocalizer != nil {
				if name := ctx.textLocalizer.thoughtName(toInt64(row["ThoughtAssetId"])); name != "" {
					return name
				}
				if name := ctx.textLocalizer.thoughtCatalogName(ctx.thoughtCatalogTerms[stringifyValue(row["ThoughtId"])]); name != "" {
					return name
				}
			}
			return fmt.Sprintf("Thought %s", stringifyValue(row["ThoughtId"]))
		},
		func(row map[string]any) string {
			return joinDetail(
				idPair("asset", row["ThoughtAssetId"]),
				idPair("ability", row["AbilityId"]),
			)
		},
	)
}

func loadWeaponEvolutionOrders(masterDataPath string) map[string]int {
	rows, err := readJSONArray(filepath.Join(masterDataPath, "EntityMWeaponEvolutionGroupTable.json"))
	if err != nil {
		return map[string]int{}
	}
	result := map[string]int{}
	for _, row := range rows {
		weaponID := stringifyValue(row["WeaponId"])
		order := int(toInt64(row["EvolutionOrder"]))
		if weaponID == "" || order == 0 {
			continue
		}
		if existing, ok := result[weaponID]; !ok || order < existing {
			result[weaponID] = order
		}
	}
	return result
}

func loadPartsGroupAssets(masterDataPath string) map[string]string {
	rows, err := readJSONArray(filepath.Join(masterDataPath, "EntityMPartsGroupTable.json"))
	if err != nil {
		return map[string]string{}
	}
	result := map[string]string{}
	for _, row := range rows {
		groupID := stringifyValue(row["PartsGroupId"])
		assetID := stringifyValue(row["PartsGroupAssetId"])
		if groupID == "" || assetID == "" {
			continue
		}
		result[groupID] = assetID
	}
	return result
}

func loadTextBundleLocalizer(assetsRoot string) *textBundleLocalizer {
	textRoot := filepath.Join(assetsRoot, "revisions", "0", "assetbundle", "text", "en")
	bundles := map[string]map[string]string{}
	for _, spec := range []struct {
		key  string
		path string
		mask string
	}{
		{"character", filepath.Join(textRoot, "character.assetbundle"), "text)en)character"},
		{"costume", filepath.Join(textRoot, "possession", "costume.assetbundle"), "text)en)possession)costume"},
		{"weapon", filepath.Join(textRoot, "possession", "weapon.assetbundle"), "text)en)possession)weapon"},
		{"companion", filepath.Join(textRoot, "possession", "companion.assetbundle"), "text)en)possession)companion"},
		{"material", filepath.Join(textRoot, "possession", "material.assetbundle"), "text)en)possession)material"},
		{"consumable", filepath.Join(textRoot, "possession", "consumable_item.assetbundle"), "text)en)possession)consumable_item"},
		{"important", filepath.Join(textRoot, "possession", "important_item.assetbundle"), "text)en)possession)important_item"},
		{"parts", filepath.Join(textRoot, "possession", "parts.assetbundle"), "text)en)possession)parts"},
		{"thought", filepath.Join(textRoot, "possession", "thought.assetbundle"), "text)en)possession)thought"},
		{"skill", filepath.Join(textRoot, "skill.assetbundle"), "text)en)skill"},
		{"ability", filepath.Join(textRoot, "ability.assetbundle"), "text)en)ability"},
		{"shop", filepath.Join(textRoot, "shop.assetbundle"), "text)en)shop"},
		{"gacha", filepath.Join(textRoot, "gacha.assetbundle"), "text)en)gacha"},
		{"gacha_title", filepath.Join(textRoot, "gacha_title.assetbundle"), "text)en)gacha_title"},
	} {
		mergeBundleEntries(bundles, spec.key, loadTextBundleEntries(spec.path, spec.mask))
	}

	for _, dirSpec := range []struct {
		key string
		dir string
	}{
		{key: "weapon", dir: filepath.Join(textRoot, "possession", "weapon")},
		{key: "thought", dir: filepath.Join(textRoot, "possession", "thought")},
		{key: "skill", dir: filepath.Join(textRoot, "skill")},
		{key: "ability", dir: filepath.Join(textRoot, "ability")},
		{key: "shop", dir: filepath.Join(textRoot, "shop")},
		{key: "gacha", dir: filepath.Join(textRoot, "gacha")},
		{key: "gacha_title", dir: filepath.Join(textRoot, "gacha_title")},
	} {
		dirEntries, err := os.ReadDir(dirSpec.dir)
		if err != nil {
			continue
		}
		for _, entry := range dirEntries {
			if entry.IsDir() || filepath.Ext(entry.Name()) != ".assetbundle" {
				continue
			}
			filePath := filepath.Join(dirSpec.dir, entry.Name())
			mask := textBundleMask(textRoot, filePath)
			mergeBundleEntries(bundles, dirSpec.key, loadTextBundleEntries(filePath, mask))
		}
	}

	if len(bundles) == 0 {
		return nil
	}
	return &textBundleLocalizer{
		bundles: bundles,
	}
}

func loadTextBundleEntries(path, mask string) map[string]string {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil
	}
	return parseTextBundleEntries(decryptTextBundle(data, mask))
}

func mergeBundleEntries(target map[string]map[string]string, bundleName string, entries map[string]string) {
	if len(entries) == 0 {
		return
	}
	if target[bundleName] == nil {
		target[bundleName] = map[string]string{}
	}
	for key, value := range entries {
		if _, exists := target[bundleName][key]; exists {
			continue
		}
		target[bundleName][key] = value
	}
}

func textBundleMask(textRoot, filePath string) string {
	relativePath, err := filepath.Rel(textRoot, filePath)
	if err != nil {
		return ""
	}
	relativePath = filepath.ToSlash(relativePath)
	relativePath = strings.TrimSuffix(relativePath, filepath.Ext(relativePath))
	return "text)en)" + strings.ReplaceAll(relativePath, "/", ")")
}

func (l *textBundleLocalizer) characterName(nameTextID int64) string {
	return l.lookup("character", strconv.FormatInt(nameTextID, 10))
}

func (l *textBundleLocalizer) costumeName(actorAssetID string) string {
	return l.lookup("costume", actorAssetID)
}

func (l *textBundleLocalizer) weaponName(actorAssetID string, evolutionOrder int) string {
	if evolutionOrder <= 0 {
		evolutionOrder = 1
	}
	if name := l.lookup("weapon", fmt.Sprintf("%s.%d", actorAssetID, evolutionOrder)); name != "" {
		return name
	}
	if name := l.lookup("weapon", fmt.Sprintf("%s.%02d", actorAssetID, evolutionOrder)); name != "" {
		return name
	}
	if nearest, diff := l.lookupNearestWeaponName(actorAssetID, evolutionOrder); nearest != "" && diff <= 1 {
		return nearest
	}
	return ""
}

func (l *textBundleLocalizer) companionName(actorAssetID string) string {
	return l.lookup("companion", actorAssetID)
}

func (l *textBundleLocalizer) materialName(categoryID, variationID int64) string {
	return l.lookup("material", fmt.Sprintf("%03d%03d", categoryID, variationID))
}

func (l *textBundleLocalizer) consumableName(categoryID, variationID int64) string {
	return l.lookup("consumable", fmt.Sprintf("%03d%03d", categoryID, variationID))
}

func (l *textBundleLocalizer) importantItemName(nameTextID int64) string {
	return l.lookup("important", fmt.Sprintf("%06d", nameTextID))
}

func (l *textBundleLocalizer) partsGroupName(assetID string) string {
	return l.lookup("parts", assetID)
}

func (l *textBundleLocalizer) thoughtName(thoughtAssetID int64) string {
	if thoughtAssetID <= 0 {
		return ""
	}
	return l.lookup("thought", fmt.Sprintf("%06d", thoughtAssetID))
}

func (l *textBundleLocalizer) thoughtCatalogName(catalogTermID int64) string {
	if catalogTermID <= 0 {
		return ""
	}
	return l.lookup("thought", fmt.Sprintf("%06d0", catalogTermID))
}

func (l *textBundleLocalizer) abilityName(textID int64) string {
	return l.lookup("ability", strconv.FormatInt(textID, 10))
}

func (l *textBundleLocalizer) skillName(textID int64) string {
	return l.lookup("skill", strconv.FormatInt(textID, 10))
}

func (l *textBundleLocalizer) shopName(textID int64) string {
	return l.lookup("shop", strconv.FormatInt(textID, 10))
}

func (l *textBundleLocalizer) lookup(bundleName, key string) string {
	if l == nil || key == "" {
		return ""
	}
	return l.bundles[bundleName][key]
}

func decryptTextBundle(buffer []byte, mask string) []byte {
	if len(buffer) == 0 || (buffer[0] != 0x31 && buffer[0] != 0x32) {
		return buffer
	}
	headerLength := 256
	if buffer[0] == 0x32 {
		headerLength = len(buffer)
	}
	maskBuffer := stringToMaskBytes(mask)
	if len(maskBuffer) == 0 {
		return buffer
	}
	output := make([]byte, len(buffer))
	copy(output, buffer)
	for i := 0; i < headerLength && i < len(output); i++ {
		output[i] = maskBuffer[i%len(maskBuffer)] ^ buffer[i]
	}
	output[0] = 0x55
	return output
}

func stringToMaskBytes(mask string) []byte {
	if mask == "" {
		return nil
	}
	output := make([]byte, len(mask)*2)
	i, j, k := 0, 0, len(output)-1
	for j < len(mask) {
		c := mask[j]
		j++
		output[i] = c
		i += 2
		output[k] = ^c
		k -= 2
	}
	maskLen := byte(0xbb)
	for a, remaining := 0, len(output); remaining > 0; remaining-- {
		maskLen = (((maskLen & 1) << 7) | (maskLen >> 1)) ^ output[a]
		a++
	}
	for idx := range output {
		output[idx] ^= maskLen
	}
	return output
}

func parseTextBundleEntries(bundle []byte) map[string]string {
	result := map[string]string{}
	previousKey := ""
	for _, entry := range scanBundleEntries(bundle) {
		fullKey := expandBundleKey(previousKey, entry.key)
		if fullKey == "" {
			continue
		}
		value := normalizeLocalizedValue(entry.value)
		if value == "" {
			continue
		}
		if _, exists := result[fullKey]; !exists {
			result[fullKey] = value
		}
		previousKey = fullKey
	}
	return result
}

type rawBundleEntry struct {
	key   string
	value string
}

func scanBundleEntries(bundle []byte) []rawBundleEntry {
	var entries []rawBundleEntry
	for idx := 0; idx < len(bundle); idx++ {
		if bundle[idx] != ':' {
			continue
		}
		keyStart := idx - 1
		for keyStart >= 0 && isBundleKeyByte(bundle[keyStart]) {
			keyStart--
		}
		keyStart++
		if keyStart >= idx {
			continue
		}

		valueStart := idx + 1
		valueEnd := valueStart
		for valueEnd < len(bundle) && isPrintableASCII(bundle[valueEnd]) {
			valueEnd++
		}
		if valueEnd <= valueStart {
			continue
		}

		key := string(bundle[keyStart:idx])
		value := string(bundle[valueStart:valueEnd])
		if !strings.ContainsAny(key, "0123456789") || !containsLetter(value) {
			continue
		}
		entries = append(entries, rawBundleEntry{
			key:   key,
			value: value,
		})
		idx = valueEnd
	}
	return entries
}

func expandBundleKey(previous, raw string) string {
	raw = strings.TrimPrefix(strings.TrimSpace(raw), ".")
	if raw == "" {
		return ""
	}
	if previous == "" || containsLetter(raw) {
		if previous != "" {
			raw = trimAbbreviatedKeyPrefix(raw)
		}
		if raw == "" || containsLetter(raw) {
			return raw
		}
	}

	if previous == "" {
		return raw
	}

	rawParts := strings.Split(raw, ".")
	prevParts := strings.Split(previous, ".")
	if len(rawParts) > len(prevParts) {
		return raw
	}

	prefixCount := len(prevParts) - len(rawParts)
	expanded := append([]string{}, prevParts[:prefixCount]...)
	for index, rawPart := range rawParts {
		prevPart := prevParts[prefixCount+index]
		if rawPart == "" {
			expanded = append(expanded, prevPart)
			continue
		}
		if isAllDigits(rawPart) {
			if expandedPart := expandNumericKeyPart(prevPart, rawPart); expandedPart != "" {
				expanded = append(expanded, expandedPart)
				continue
			}
			expanded = append(expanded, replaceTrailingDigits(prevPart, rawPart))
			continue
		}
		expanded = append(expanded, rawPart)
	}
	return strings.Join(expanded, ".")
}

func trimAbbreviatedKeyPrefix(value string) string {
	if len(value) < 2 {
		return value
	}
	first := value[0]
	if !((first >= 'A' && first <= 'Z') || (first >= 'a' && first <= 'z')) {
		return value
	}
	for index := 1; index < len(value); index++ {
		current := value[index]
		if current >= '0' && current <= '9' {
			return value[index:]
		}
		if current != '.' {
			return value
		}
	}
	return value
}

func expandNumericKeyPart(previous, raw string) string {
	prefix, digits, ok := splitTrailingDigits(previous)
	if !ok {
		return ""
	}
	if len(digits) == 6 {
		switch {
		case len(raw) <= 3:
			return prefix + digits[:3] + leftPadDigits(raw, 3)
		case len(raw) <= 6:
			typeDigits := raw[:len(raw)-3]
			variationDigits := raw[len(raw)-3:]
			return prefix + leftPadDigits(typeDigits, 3) + leftPadDigits(variationDigits, 3)
		}
	}
	return ""
}

func splitTrailingDigits(value string) (string, string, bool) {
	end := len(value)
	for end > 0 && value[end-1] >= '0' && value[end-1] <= '9' {
		end--
	}
	if end == len(value) {
		return "", "", false
	}
	return value[:end], value[end:], true
}

func leftPadDigits(value string, width int) string {
	if len(value) >= width {
		return value[len(value)-width:]
	}
	return strings.Repeat("0", width-len(value)) + value
}

func replaceTrailingDigits(previous, suffix string) string {
	end := len(previous)
	for end > 0 && previous[end-1] >= '0' && previous[end-1] <= '9' {
		end--
	}
	digitCount := len(previous) - end
	if digitCount == 0 {
		return previous + suffix
	}
	if len(suffix) > digitCount {
		return previous[:end] + suffix
	}
	return previous[:len(previous)-len(suffix)] + suffix
}

func normalizeLocalizedValue(value string) string {
	value = strings.TrimSpace(value)
	start := 0
	for start < len(value) && !isAlphaNumeric(value[start]) {
		start++
	}
	end := len(value)
	for end > start && !isAlphaNumeric(value[end-1]) {
		end--
	}
	value = value[start:end]
	value = strings.Join(strings.Fields(value), " ")
	value = trimLocalizationArtifact(value)
	return value
}

func trimLocalizationArtifact(value string) string {
	if strings.Count(value, " ") < 2 || len(value) < 2 {
		return value
	}
	last := value[len(value)-1]
	if last < '0' || last > '9' {
		return value
	}
	prev := value[len(value)-2]
	if (prev < 'A' || prev > 'Z') && (prev < 'a' || prev > 'z') {
		return value
	}
	return value[:len(value)-1]
}

func containsLetter(value string) bool {
	for _, r := range value {
		if ('A' <= r && r <= 'Z') || ('a' <= r && r <= 'z') {
			return true
		}
	}
	return false
}

func isPrintableASCII(value byte) bool {
	return value >= 0x20 && value <= 0x7e
}

func isBundleKeyByte(value byte) bool {
	return (value >= '0' && value <= '9') ||
		(value >= 'A' && value <= 'Z') ||
		(value >= 'a' && value <= 'z') ||
		value == '.' || value == '_' || value == '-'
}

func isAllDigits(value string) bool {
	if value == "" {
		return false
	}
	for _, r := range value {
		if r < '0' || r > '9' {
			return false
		}
	}
	return true
}

func isAlphaNumeric(value byte) bool {
	return (value >= '0' && value <= '9') ||
		(value >= 'A' && value <= 'Z') ||
		(value >= 'a' && value <= 'z')
}

func costumeActorAssetID(row map[string]any) string {
	categoryPrefix := "mt"
	if toInt64(row["CostumeAssetCategoryType"]) == 1 {
		categoryPrefix = "ch"
	}
	return fmt.Sprintf("%s%03d%03d", categoryPrefix, toInt64(row["ActorSkeletonId"]), toInt64(row["AssetVariationId"]))
}

func weaponActorAssetID(row map[string]any) string {
	categoryPrefix := "mw"
	if toInt64(row["WeaponCategoryType"]) == 1 {
		categoryPrefix = "wp"
	}
	return fmt.Sprintf("%s%03d%03d", categoryPrefix, toInt64(row["WeaponType"]), toInt64(row["AssetVariationId"]))
}

func weaponNameAssetIDs(row map[string]any) []string {
	weaponID := toInt64(row["WeaponId"])
	weaponType := toInt64(row["WeaponType"])
	ids := []string{}
	if override := weaponNameAssetIDOverride(weaponID); override != "" {
		ids = append(ids, override)
	}
	if weaponID > 0 && weaponType > 0 {
		categoryPrefix := "mw"
		if toInt64(row["WeaponCategoryType"]) == 1 {
			categoryPrefix = "wp"
		}
		ids = append(ids, fmt.Sprintf("%s%03d%03d", categoryPrefix, weaponType, weaponID%1000))
	}

	assetID := weaponActorAssetID(row)
	for _, existing := range ids {
		if existing == assetID {
			return ids
		}
	}
	ids = append(ids, assetID)
	return ids
}

func weaponNameAssetIDOverride(weaponID int64) string {
	switch weaponID {
	case 101001:
		return "wp005533"
	case 101011:
		return "wp001035"
	case 101021:
		return "wp006017"
	case 101031:
		return "wp003039"
	case 101041:
		return "wp002046"
	default:
		return ""
	}
}

func (l *textBundleLocalizer) lookupNearestWeaponName(actorAssetID string, evolutionOrder int) (string, int) {
	if l == nil || actorAssetID == "" {
		return "", 0
	}
	entries := l.bundles["weapon"]
	if len(entries) == 0 {
		return "", 0
	}

	prefix, digits, ok := splitTrailingDigits(actorAssetID)
	if !ok || len(digits) != 6 {
		return "", 0
	}

	targetFamily := digits[:3]
	targetVariation, err := strconv.Atoi(digits[3:])
	if err != nil {
		return "", 0
	}

	bestName := ""
	bestDiff := 1 << 30
	bestVariation := 1 << 30
	for key, value := range entries {
		parts := strings.Split(key, ".")
		if len(parts) != 2 {
			continue
		}
		order, err := strconv.Atoi(parts[1])
		if err != nil || order != evolutionOrder {
			continue
		}
		keyPrefix, keyDigits, ok := splitTrailingDigits(parts[0])
		if !ok || keyPrefix != prefix || len(keyDigits) != 6 || keyDigits[:3] != targetFamily {
			continue
		}
		variation, err := strconv.Atoi(keyDigits[3:])
		if err != nil {
			continue
		}
		diff := variation - targetVariation
		if diff < 0 {
			diff = -diff
		}
		if diff < bestDiff || (diff == bestDiff && variation < bestVariation) {
			bestName = value
			bestDiff = diff
			bestVariation = variation
		}
	}
	if bestName == "" {
		return "", 0
	}
	return bestName, bestDiff
}

func companionActorAssetID(row map[string]any) string {
	return fmt.Sprintf("cm%03d%03d", toInt64(row["ActorSkeletonId"]), toInt64(row["AssetVariationId"]))
}

func (l *lookupIndex) loadFile(basePath, filename, idKey, column string, labelFn, detailFn func(map[string]any) string) error {
	rows, err := readJSONArray(filepath.Join(basePath, filename))
	if err != nil {
		return fmt.Errorf("load %s: %w", filename, err)
	}
	if l.columns[column] == nil {
		l.columns[column] = map[string]lookupEntry{}
	}
	for _, row := range rows {
		id := stringifyValue(row[idKey])
		if id == "" {
			continue
		}
		l.columns[column][id] = lookupEntry{
			Label:  labelFn(row),
			Detail: detailFn(row),
		}
	}
	return nil
}

func loadCharacterNames(masterDataPath string, textLocalizer *textBundleLocalizer) map[string]string {
	rows, err := readJSONArray(filepath.Join(masterDataPath, "EntityMCharacterTable.json"))
	if err != nil {
		return map[string]string{}
	}
	result := map[string]string{}
	for _, row := range rows {
		characterID := stringifyValue(row["CharacterId"])
		if characterID == "" {
			continue
		}
		name := fmt.Sprintf("Character %s", characterID)
		if textLocalizer != nil {
			if localized := textLocalizer.characterName(toInt64(row["NameCharacterTextId"])); localized != "" {
				name = localized
			}
		}
		result[characterID] = name
	}
	return result
}

func loadThoughtCatalogTerms(masterDataPath string) map[string]int64 {
	rows, err := readJSONArray(filepath.Join(masterDataPath, "EntityMCatalogThoughtTable.json"))
	if err != nil {
		return map[string]int64{}
	}
	result := map[string]int64{}
	for _, row := range rows {
		thoughtID := stringifyValue(row["ThoughtId"])
		if thoughtID == "" {
			continue
		}
		result[thoughtID] = toInt64(row["CatalogTermId"])
	}
	return result
}

func (l *lookupIndex) loadAbilityLookups(masterDataPath string, textLocalizer *textBundleLocalizer) error {
	detailRows, err := readJSONArray(filepath.Join(masterDataPath, "EntityMAbilityDetailTable.json"))
	if err != nil {
		return err
	}

	detailByID := map[string]map[string]any{}
	for _, row := range detailRows {
		detailByID[stringifyValue(row["AbilityDetailId"])] = row
	}

	data, err := os.ReadFile(filepath.Join(masterDataPath, "m_abiliwy.json"))
	if err != nil {
		return err
	}
	var pairs [][]any
	if err := json.Unmarshal(data, &pairs); err != nil {
		return err
	}

	l.columns["ability_id"] = map[string]lookupEntry{}
	for _, pair := range pairs {
		if len(pair) < 2 {
			continue
		}
		abilityID := stringifyValue(pair[0])
		detailID := stringifyValue(pair[1])
		detailRow := detailByID[detailID]
		if abilityID == "" || detailRow == nil {
			continue
		}

		label := fmt.Sprintf("Ability %s", abilityID)
		if textLocalizer != nil {
			if localized := textLocalizer.abilityName(toInt64(detailRow["NameAbilityTextId"])); localized != "" {
				label = localized
			}
		}

		l.columns["ability_id"][abilityID] = lookupEntry{
			Label: label,
			Detail: joinDetail(
				textPair("name text", detailRow["NameAbilityTextId"]),
				textPair("description text", detailRow["DescriptionAbilityTextId"]),
			),
		}
	}
	return nil
}

func (l *lookupIndex) loadSkillLookups(masterDataPath string, textLocalizer *textBundleLocalizer) error {
	skillRows, err := readJSONArray(filepath.Join(masterDataPath, "EntityMSkillTable.json"))
	if err != nil {
		return err
	}
	levelRows, err := readJSONArray(filepath.Join(masterDataPath, "EntityMSkillLevelGroupTable.json"))
	if err != nil {
		return err
	}
	detailRows, err := readJSONArray(filepath.Join(masterDataPath, "EntityMSkillDetailTable.json"))
	if err != nil {
		return err
	}

	groupToDetail := map[string]map[string]any{}
	for _, row := range levelRows {
		groupID := stringifyValue(row["SkillLevelGroupId"])
		if groupID == "" {
			continue
		}
		existing := groupToDetail[groupID]
		if existing == nil || toInt64(row["LevelLowerLimit"]) < toInt64(existing["LevelLowerLimit"]) {
			groupToDetail[groupID] = row
		}
	}

	detailByID := map[string]map[string]any{}
	for _, row := range detailRows {
		detailByID[stringifyValue(row["SkillDetailId"])] = row
	}

	l.columns["skill_id"] = map[string]lookupEntry{}
	for _, row := range skillRows {
		skillID := stringifyValue(row["SkillId"])
		groupID := stringifyValue(row["SkillLevelGroupId"])
		groupRow := groupToDetail[groupID]
		detailRow := detailByID[stringifyValue(groupRow["SkillDetailId"])]
		if skillID == "" || detailRow == nil {
			continue
		}

		label := fmt.Sprintf("Skill %s", skillID)
		if textLocalizer != nil {
			if localized := textLocalizer.skillName(toInt64(detailRow["NameSkillTextId"])); localized != "" {
				label = localized
			}
		}

		l.columns["skill_id"][skillID] = lookupEntry{
			Label: label,
			Detail: joinDetail(
				textPair("name text", detailRow["NameSkillTextId"]),
				textPair("description text", detailRow["DescriptionSkillTextId"]),
			),
		}
	}
	return nil
}

func (l *lookupIndex) loadGiftTextLookups(masterDataPath string) error {
	rows, err := readJSONArray(filepath.Join(masterDataPath, "EntityMGiftTextTable.json"))
	if err != nil {
		return err
	}
	l.columns["description_gift_text_id"] = map[string]lookupEntry{}
	for _, row := range rows {
		if toInt64(row["LanguageType"]) != 2 {
			continue
		}
		textID := stringifyValue(row["GiftTextId"])
		text := strings.TrimSpace(stringifyValue(row["Text"]))
		if textID == "" || text == "" || text == "-" {
			continue
		}
		l.columns["description_gift_text_id"][textID] = lookupEntry{Label: text}
	}
	return nil
}

func (l *lookupIndex) loadShopItemLookups(masterDataPath string, textLocalizer *textBundleLocalizer) error {
	rows, err := readJSONArray(filepath.Join(masterDataPath, "EntityMShopItemTable.json"))
	if err != nil {
		return err
	}
	l.columns["shop_item_id"] = map[string]lookupEntry{}
	for _, row := range rows {
		shopItemID := stringifyValue(row["ShopItemId"])
		if shopItemID == "" {
			continue
		}
		label := fmt.Sprintf("Shop Item %s", shopItemID)
		if textLocalizer != nil {
			if localized := textLocalizer.shopName(toInt64(row["NameShopTextId"])); localized != "" {
				label = localized
			}
		}
		l.columns["shop_item_id"][shopItemID] = lookupEntry{
			Label: label,
			Detail: joinDetail(
				textPair("name text", row["NameShopTextId"]),
				textPair("description text", row["DescriptionShopTextId"]),
				idPair("price item", row["PriceId"]),
				idPair("price", row["Price"]),
			),
		}
	}
	return nil
}

func (l *lookupIndex) loadCharacterBoardLookups(masterDataPath string, characterNames map[string]string) error {
	boardRows, err := readJSONArray(filepath.Join(masterDataPath, "EntityMCharacterBoardTable.json"))
	if err != nil {
		return err
	}
	groupRows, err := readJSONArray(filepath.Join(masterDataPath, "EntityMCharacterBoardGroupTable.json"))
	if err != nil {
		return err
	}
	assignments, err := readJSONArray(filepath.Join(masterDataPath, "EntityMCharacterBoardAssignmentTable.json"))
	if err != nil {
		return err
	}

	groupByID := map[string]map[string]any{}
	for _, row := range groupRows {
		groupByID[stringifyValue(row["CharacterBoardGroupId"])] = row
	}

	characterByCategoryID := map[string]string{}
	for _, row := range assignments {
		categoryID := stringifyValue(row["CharacterBoardCategoryId"])
		characterID := stringifyValue(row["CharacterId"])
		if categoryID == "" || characterID == "" {
			continue
		}
		if _, exists := characterByCategoryID[categoryID]; !exists {
			characterByCategoryID[categoryID] = characterID
		}
	}

	l.columns["character_board_id"] = map[string]lookupEntry{}
	for _, row := range boardRows {
		boardID := stringifyValue(row["CharacterBoardId"])
		groupRow := groupByID[stringifyValue(row["CharacterBoardGroupId"])]
		characterID := stringifyValue(groupRow["CharacterBoardCategoryId"])
		if groupRow != nil {
			characterID = characterByCategoryID[stringifyValue(groupRow["CharacterBoardCategoryId"])]
		}

		label := fmt.Sprintf("Character Board %s", boardID)
		if name := characterNames[characterID]; name != "" {
			label = name + " Board"
		}

		l.columns["character_board_id"][boardID] = lookupEntry{
			Label: label,
			Detail: joinDetail(
				idPair("character", characterID),
				idPair("group type", groupRow["CharacterBoardGroupType"]),
				idPair("release rank", row["ReleaseRank"]),
			),
		}
	}
	return nil
}

func (l *lookupIndex) loadWeaponSlotLookups(masterDataPath string) error {
	weaponRows, err := readJSONArray(filepath.Join(masterDataPath, "EntityMWeaponTable.json"))
	if err != nil {
		return err
	}
	skillGroupRows, err := readJSONArray(filepath.Join(masterDataPath, "EntityMWeaponSkillGroupTable.json"))
	if err != nil {
		return err
	}
	abilityGroupRows, err := readJSONArray(filepath.Join(masterDataPath, "EntityMWeaponAbilityGroupTable.json"))
	if err != nil {
		return err
	}

	skillGroupSlots := map[string]map[int]lookupEntry{}
	for _, row := range skillGroupRows {
		groupID := stringifyValue(row["WeaponSkillGroupId"])
		slot := int(toInt64(row["SlotNumber"]))
		entry, ok := l.lookupExact("skill_id", stringifyValue(row["SkillId"]))
		if groupID == "" || slot == 0 || !ok {
			continue
		}
		if skillGroupSlots[groupID] == nil {
			skillGroupSlots[groupID] = map[int]lookupEntry{}
		}
		skillGroupSlots[groupID][slot] = entry
	}

	abilityGroupSlots := map[string]map[int]lookupEntry{}
	for _, row := range abilityGroupRows {
		groupID := stringifyValue(row["WeaponAbilityGroupId"])
		slot := int(toInt64(row["SlotNumber"]))
		entry, ok := l.lookupExact("ability_id", stringifyValue(row["AbilityId"]))
		if groupID == "" || slot == 0 || !ok {
			continue
		}
		if abilityGroupSlots[groupID] == nil {
			abilityGroupSlots[groupID] = map[int]lookupEntry{}
		}
		abilityGroupSlots[groupID][slot] = entry
	}

	for _, row := range weaponRows {
		weaponID := stringifyValue(row["WeaponId"])
		if weaponID == "" {
			continue
		}
		if slots := skillGroupSlots[stringifyValue(row["WeaponSkillGroupId"])]; len(slots) > 0 {
			l.weaponSkillSlots[weaponID] = slots
		}
		if slots := abilityGroupSlots[stringifyValue(row["WeaponAbilityGroupId"])]; len(slots) > 0 {
			l.weaponAbilitySlots[weaponID] = slots
		}
	}
	return nil
}

func (l *lookupIndex) loadCostumeActiveSkillLookups(masterDataPath string) error {
	costumeRows, err := readJSONArray(filepath.Join(masterDataPath, "EntityMCostumeTable.json"))
	if err != nil {
		return err
	}
	groupRows, err := readJSONArray(filepath.Join(masterDataPath, "EntityMCostumeActiveSkillGroupTable.json"))
	if err != nil {
		return err
	}

	groupEntries := map[string]map[int]lookupEntry{}
	for _, row := range groupRows {
		groupID := stringifyValue(row["CostumeActiveSkillGroupId"])
		limitBreak := int(toInt64(row["CostumeLimitBreakCountLowerLimit"]))
		skillID := stringifyValue(row["CostumeActiveSkillId"])
		entry, ok := l.lookupExact("skill_id", skillID)
		if !ok {
			entry = lookupEntry{Label: fmt.Sprintf("Active Skill %s", skillID)}
		}
		if groupEntries[groupID] == nil {
			groupEntries[groupID] = map[int]lookupEntry{}
		}
		groupEntries[groupID][limitBreak] = entry
	}

	for _, row := range costumeRows {
		costumeID := stringifyValue(row["CostumeId"])
		groupID := stringifyValue(row["CostumeActiveSkillGroupId"])
		if costumeID == "" || groupID == "" {
			continue
		}
		if entries := groupEntries[groupID]; len(entries) > 0 {
			l.costumeActiveSkills[costumeID] = entries
		}
	}
	return nil
}

func (l *lookupIndex) annotateRow(table string, row map[string]any, context *userLookupContext) map[string]lookupEntry {
	if l == nil || !l.summary.Enabled {
		return nil
	}
	annotations := map[string]lookupEntry{}
	for column, value := range row {
		if entry, ok := l.lookupColumn(column, value); ok {
			annotations[column] = entry
		}
	}
	if context != nil {
		for column, value := range row {
			if entry, ok := context.lookupColumn(column, value); ok {
				annotations[column] = entry
			}
		}
		for column, entry := range context.tableAnnotations(table, row) {
			annotations[column] = entry
		}
	}
	if len(annotations) == 0 {
		return nil
	}
	return annotations
}

func (l *lookupIndex) lookupColumn(column string, value any) (lookupEntry, bool) {
	id := strings.TrimSpace(stringifyValue(value))
	if id == "" || id == "0" {
		return lookupEntry{}, false
	}
	if canonical := canonicalNierReinLookupColumn(column); canonical != "" {
		return l.lookupExact(canonical, id)
	}
	if entry, ok := l.lookupExact(column, id); ok {
		return entry, true
	}
	for suffix, entries := range l.columns {
		if strings.HasSuffix(column, suffix) {
			entry, ok := entries[id]
			return entry, ok
		}
	}
	return lookupEntry{}, false
}

func (l *lookupIndex) lookupExact(column, id string) (lookupEntry, bool) {
	entries, ok := l.columns[column]
	if !ok {
		return lookupEntry{}, false
	}
	entry, ok := entries[id]
	return entry, ok
}

func (l *lookupIndex) costumeActiveSkillForCostume(costumeID string, limitBreak int) lookupEntry {
	if l == nil {
		return lookupEntry{}
	}
	entries := l.costumeActiveSkills[costumeID]
	bestLimit := -1
	best := lookupEntry{}
	for threshold, entry := range entries {
		if threshold <= limitBreak && threshold > bestLimit {
			bestLimit = threshold
			best = entry
		}
	}
	if best.Label != "" {
		return best
	}
	return entries[0]
}

func emptyUserLookupContext() *userLookupContext {
	return &userLookupContext{
		columns:           map[string]map[string]lookupEntry{},
		weaponRefs:        map[string]ownedEntityRef{},
		costumeRefs:       map[string]ownedEntityRef{},
		companionRefs:     map[string]ownedEntityRef{},
		partsRefs:         map[string]ownedEntityRef{},
		thoughtRefs:       map[string]ownedEntityRef{},
		deckCharacterRefs: map[string]lookupEntry{},
	}
}

func loadUserLookupContext(dbPath, userID string, lookups *lookupIndex) *userLookupContext {
	if strings.TrimSpace(userID) == "" || lookups == nil {
		return emptyUserLookupContext()
	}

	ctx := emptyUserLookupContext()
	ctx.columns["user_weapon_uuid"] = map[string]lookupEntry{}
	ctx.columns["main_user_weapon_uuid"] = ctx.columns["user_weapon_uuid"]
	ctx.columns["user_costume_uuid"] = map[string]lookupEntry{}
	ctx.columns["user_companion_uuid"] = map[string]lookupEntry{}
	ctx.columns["user_parts_uuid"] = map[string]lookupEntry{}
	ctx.columns["user_thought_uuid"] = map[string]lookupEntry{}
	ctx.columns["user_deck_character_uuid"] = map[string]lookupEntry{}

	loadOwnedEntities := func(table, uuidColumn, idColumn string, target map[string]ownedEntityRef, columnEntries map[string]lookupEntry, enrich func(string, map[string]any) ownedEntityRef) {
		rows, err := querySQLiteJSON(dbPath, fmt.Sprintf(
			"SELECT * FROM %s WHERE user_id = %s ORDER BY ROWID",
			table,
			sqlLiteral(columnInfo{Type: "INTEGER"}, userID),
		))
		if err != nil {
			return
		}
		for _, row := range rows {
			uuid := stringifyValue(row[uuidColumn])
			entityID := stringifyValue(row[idColumn])
			if uuid == "" || entityID == "" {
				continue
			}
			ref := enrich(entityID, row)
			if ref.entry.Label == "" {
				ref.entry.Label = fmt.Sprintf("%s %s", strings.TrimPrefix(idColumn, "user_"), entityID)
			}
			ref.entityID = entityID
			target[uuid] = ref
			columnEntries[uuid] = ref.entry
		}
	}

	loadOwnedEntities("user_weapons", "user_weapon_uuid", "weapon_id", ctx.weaponRefs, ctx.columns["user_weapon_uuid"], func(weaponID string, row map[string]any) ownedEntityRef {
		entry, _ := lookups.lookupExact("weapon_id", weaponID)
		return ownedEntityRef{
			entry:        entry,
			skillSlots:   lookups.weaponSkillSlots[weaponID],
			abilitySlots: lookups.weaponAbilitySlots[weaponID],
		}
	})

	loadOwnedEntities("user_costumes", "user_costume_uuid", "costume_id", ctx.costumeRefs, ctx.columns["user_costume_uuid"], func(costumeID string, row map[string]any) ownedEntityRef {
		entry, _ := lookups.lookupExact("costume_id", costumeID)
		limitBreak := int(toInt64(row["limit_break_count"]))
		activeSkill := lookups.costumeActiveSkillForCostume(costumeID, limitBreak)
		if activeSkill.Label != "" {
			entry.Detail = joinDetail(entry.Detail, "active skill "+activeSkill.Label)
		}
		return ownedEntityRef{
			entry:       entry,
			limitBreak:  limitBreak,
			activeSkill: activeSkill,
		}
	})

	loadOwnedEntities("user_companions", "user_companion_uuid", "companion_id", ctx.companionRefs, ctx.columns["user_companion_uuid"], func(companionID string, _ map[string]any) ownedEntityRef {
		entry, _ := lookups.lookupExact("companion_id", companionID)
		return ownedEntityRef{entry: entry}
	})
	loadOwnedEntities("user_parts", "user_parts_uuid", "parts_id", ctx.partsRefs, ctx.columns["user_parts_uuid"], func(partsID string, _ map[string]any) ownedEntityRef {
		entry, _ := lookups.lookupExact("parts_id", partsID)
		return ownedEntityRef{entry: entry}
	})
	loadOwnedEntities("user_thoughts", "user_thought_uuid", "thought_id", ctx.thoughtRefs, ctx.columns["user_thought_uuid"], func(thoughtID string, _ map[string]any) ownedEntityRef {
		entry, _ := lookups.lookupExact("thought_id", thoughtID)
		return ownedEntityRef{entry: entry}
	})

	rows, err := querySQLiteJSON(dbPath, fmt.Sprintf(
		"SELECT * FROM user_deck_characters WHERE user_id = %s ORDER BY ROWID",
		sqlLiteral(columnInfo{Type: "INTEGER"}, userID),
	))
	if err == nil {
		for _, row := range rows {
			deckUUID := stringifyValue(row["user_deck_character_uuid"])
			if deckUUID == "" {
				continue
			}
			entry := lookupEntry{Label: fmt.Sprintf("Deck Character %s", deckUUID)}
			if costumeEntry, ok := ctx.lookupColumn("user_costume_uuid", row["user_costume_uuid"]); ok {
				entry.Label = costumeEntry.Label
				entry.Detail = joinDetail("deck character", costumeEntry.Detail)
			}
			ctx.deckCharacterRefs[deckUUID] = entry
			ctx.columns["user_deck_character_uuid"][deckUUID] = entry
		}
	}

	return ctx
}

func (c *userLookupContext) lookupColumn(column string, value any) (lookupEntry, bool) {
	if c == nil {
		return lookupEntry{}, false
	}
	id := strings.TrimSpace(stringifyValue(value))
	if id == "" || id == "0" {
		return lookupEntry{}, false
	}
	entries, ok := c.columns[column]
	if !ok {
		return lookupEntry{}, false
	}
	entry, ok := entries[id]
	return entry, ok
}

func (c *userLookupContext) tableAnnotations(table string, row map[string]any) map[string]lookupEntry {
	if c == nil {
		return nil
	}
	annotations := map[string]lookupEntry{}
	switch table {
	case "user_weapon_abilities":
		if ref, ok := c.weaponRefs[stringifyValue(row["user_weapon_uuid"])]; ok {
			if entry, ok := ref.abilitySlots[int(toInt64(row["slot_number"]))]; ok {
				annotations["slot_number"] = lookupEntry{Label: entry.Label}
			}
		}
	case "user_weapon_skills":
		if ref, ok := c.weaponRefs[stringifyValue(row["user_weapon_uuid"])]; ok {
			if entry, ok := ref.skillSlots[int(toInt64(row["slot_number"]))]; ok {
				annotations["slot_number"] = lookupEntry{Label: entry.Label}
			}
		}
	case "user_costume_active_skills":
		if ref, ok := c.costumeRefs[stringifyValue(row["user_costume_uuid"])]; ok && ref.activeSkill.Label != "" {
			annotations["level"] = lookupEntry{Label: ref.activeSkill.Label}
		}
	}
	if len(annotations) == 0 {
		return nil
	}
	return annotations
}

func readJSONArray(path string) ([]map[string]any, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var rows []map[string]any
	if err := json.Unmarshal(data, &rows); err != nil {
		return nil, err
	}
	return rows, nil
}

func joinDetail(parts ...string) string {
	filtered := make([]string, 0, len(parts))
	for _, part := range parts {
		if strings.TrimSpace(part) != "" {
			filtered = append(filtered, part)
		}
	}
	return strings.Join(filtered, " · ")
}

func idPair(label string, value any) string {
	text := strings.TrimSpace(stringifyValue(value))
	if text == "" || text == "0" {
		return ""
	}
	return fmt.Sprintf("%s %s", label, text)
}

func textPair(label string, value any) string {
	text := strings.TrimSpace(stringifyValue(value))
	if text == "" || text == "0" {
		return ""
	}
	return fmt.Sprintf("%s %s", label, text)
}

func dateRange(startValue, endValue any) string {
	start := toInt64(startValue)
	end := toInt64(endValue)
	if start == 0 && end == 0 {
		return ""
	}
	return fmt.Sprintf("%s to %s", formatUnixMillis(start), formatUnixMillis(end))
}

func formatUnixMillis(value int64) string {
	if value <= 0 {
		return "unknown"
	}
	return time.UnixMilli(value).UTC().Format("2006-01-02")
}
