const state = {
  overview: null,
  users: [],
  selectedUserId: null,
  selectedTable: null,
  tableRows: [],
  tableAnnotations: [],
  tableSchema: null,
  tableSearch: "",
  lookupOptions: {},
};

const els = {
  dbPath: document.getElementById("db-path"),
  overviewStats: document.getElementById("overview-stats"),
  userList: document.getElementById("user-list"),
  usersEmpty: document.getElementById("users-empty"),
  refreshUsers: document.getElementById("refresh-users"),
  heroTitle: document.getElementById("hero-title"),
  heroSubtitle: document.getElementById("hero-subtitle"),
  userSummaryPanel: document.getElementById("user-summary-panel"),
  userSummaryGrid: document.getElementById("user-summary-grid"),
  deleteUser: document.getElementById("delete-user"),
  tablePanel: document.getElementById("table-panel"),
  tableHeading: document.getElementById("table-heading"),
  tableSearch: document.getElementById("table-search"),
  tableSelect: document.getElementById("table-select"),
  addRow: document.getElementById("add-row"),
  editorTable: document.getElementById("editor-table"),
  tableEmpty: document.getElementById("table-empty"),
  tableMeta: document.getElementById("table-meta"),
  lookupStatus: document.getElementById("lookup-status"),
};

async function fetchJSON(url, options = {}) {
  const response = await fetch(url, {
    headers: { "Content-Type": "application/json" },
    ...options,
  });
  if (!response.ok) {
    const text = await response.text();
    throw new Error(text || `Request failed: ${response.status}`);
  }
  return response.json();
}

function formatNumber(value) {
  return new Intl.NumberFormat().format(Number(value || 0));
}

function tableLabel(name) {
  return name.replace(/^user_/, "").replaceAll("_", " ");
}

function renderOverview() {
  els.dbPath.textContent = state.overview.dbPath;
  const lookup = state.overview.lookupSummary || {};
  if (lookup.enabled) {
    els.lookupStatus.textContent =
      `Master-data annotations loaded from ${lookup.sourcePath} for ${formatNumber(lookup.entryCount)} mapped IDs across ${lookup.kinds?.length || 0} lookup groups.`;
  } else {
    els.lookupStatus.textContent =
      "Master-data annotations are not loaded. The editor will still work, but ID cells will stay raw.";
  }
  const stats = [
    ["Users", state.overview.userCount],
    ["Tables", state.overview.tableCount],
    ["Weapons", state.overview.rowCounts.user_weapons || 0],
    ["Quests", state.overview.rowCounts.user_quests || 0],
  ];

  els.overviewStats.innerHTML = "";
  for (const [label, value] of stats) {
    const card = document.createElement("div");
    card.className = "stat-card";
    card.innerHTML = `<span class="muted">${label}</span><span class="value">${formatNumber(value)}</span>`;
    els.overviewStats.append(card);
  }
}

function renderUsers() {
  els.userList.innerHTML = "";
  els.usersEmpty.classList.toggle("hidden", state.users.length > 0);

  for (const user of state.users) {
    const button = document.createElement("button");
    button.className = "user-card";
    if (String(user.userId) === String(state.selectedUserId)) {
      button.classList.add("active");
    }
    const name = user.name || `User ${user.userId}`;
    button.innerHTML = `
      <div class="title">${name}</div>
      <div class="details">ID ${user.userId} · Lv ${user.level} · Gems ${formatNumber((user.paidGem || 0) + (user.freeGem || 0))}</div>
      <div class="details mono">${user.uuid || "No UUID"}</div>
    `;
    button.addEventListener("click", () => selectUser(user.userId));
    els.userList.append(button);
  }
}

function populateTableSelect() {
  els.tableSelect.innerHTML = "";
  const groups = state.overview.tableGroups || [];
  const search = state.tableSearch.trim().toLowerCase();
  let firstVisibleValue = "";
  for (const group of groups) {
    const matchingTables = group.tables.filter((table) => {
      if (!state.overview.schema[table]) return false;
      if (!search) return true;
      const label = tableLabel(table);
      return (
        table.toLowerCase().includes(search) ||
        label.toLowerCase().includes(search) ||
        group.label.toLowerCase().includes(search)
      );
    });
    if (matchingTables.length === 0) continue;
    const optgroup = document.createElement("optgroup");
    optgroup.label = group.label;
    for (const table of matchingTables) {
      const option = document.createElement("option");
      option.value = table;
      option.textContent = tableLabel(table);
      if (!firstVisibleValue) firstVisibleValue = table;
      optgroup.append(option);
    }
    els.tableSelect.append(optgroup);
  }
  const optionValues = [...els.tableSelect.querySelectorAll("option")].map((option) => option.value);
  if (!optionValues.includes(state.selectedTable)) {
    state.selectedTable = firstVisibleValue || "";
  }
  els.tableSelect.value = state.selectedTable;
}

function renderUserSummary(user) {
  state.selectedUserId = user.userId;
  els.heroTitle.textContent = user.name || `User ${user.userId}`;
  els.heroSubtitle.textContent = user.message || "No profile message saved.";
  els.userSummaryPanel.classList.remove("hidden");
  els.tablePanel.classList.remove("hidden");
  els.userSummaryGrid.innerHTML = "";

  const cards = [
    ["User ID", user.userId],
    ["Player ID", user.playerId],
    ["Level", user.level],
    ["Experience", user.exp],
    ["Paid Gems", user.paidGem],
    ["Free Gems", user.freeGem],
    ["Latest Version", user.latestVersion],
  ];

  for (const [label, value] of cards) {
    const card = document.createElement("div");
    card.className = "summary-card";
    card.innerHTML = `<span class="muted">${label}</span><span class="value">${formatNumber(value)}</span>`;
    els.userSummaryGrid.append(card);
  }
}

function currentTableIsUserScoped() {
  const schema = state.overview?.schema?.[state.selectedTable];
  return Boolean(schema?.columns?.some((column) => column.name === "user_id"));
}

function defaultValueForColumn(column) {
  if (column.name === "user_id") return state.selectedUserId;
  if (column.type.toUpperCase().includes("INT")) return "0";
  return "";
}

function buildBlankRow() {
  const row = {};
  for (const column of state.tableSchema.columns) {
    row[column.name] = defaultValueForColumn(column);
  }
  return row;
}

async function ensureLookupOptions(column) {
  if (state.lookupOptions[column]) {
    return state.lookupOptions[column];
  }
  const data = await fetchJSON(`/api/lookups/${column}`);
  state.lookupOptions[column] = data.options || [];
  return state.lookupOptions[column];
}

function buildEditorControl(column, row) {
  if (column.name === "character_id") {
    const select = document.createElement("select");
    select.className = "table-select-input";
    select.dataset.column = column.name;
    select.disabled = column.name === "user_id";

    const currentValue = String(row[column.name] ?? "");
    const placeholder = document.createElement("option");
    placeholder.value = "";
    placeholder.textContent = currentValue ? `Character ${currentValue}` : "Select character";
    if (!currentValue) {
      placeholder.selected = true;
    }
    select.append(placeholder);

    for (const option of state.lookupOptions.character_id || []) {
      const el = document.createElement("option");
      el.value = option.value;
      el.textContent = option.label ? `${option.label} (${option.value})` : option.value;
      if (option.value === currentValue) {
        el.selected = true;
      }
      select.append(el);
    }
    if (currentValue && !select.value) {
      select.value = currentValue;
    }
    return select;
  }

  const input = document.createElement("input");
  input.className = "table-input";
  input.value = row[column.name] ?? "";
  input.dataset.column = column.name;
  input.disabled = column.name === "user_id";
  return input;
}

function renderTable() {
  const { columns, primaryKey } = state.tableSchema;
  els.tableHeading.textContent = tableLabel(state.selectedTable);
  const scopeLabel = currentTableIsUserScoped()
    ? `filtered to user ${state.selectedUserId ?? "all"}`
    : "global table";
  const category = state.overview.tableGroups.find((group) => group.tables.includes(state.selectedTable))?.label || "Uncategorized";
  els.tableMeta.textContent = `Category: ${category} · Primary key: ${primaryKey.join(", ") || "none"} · ${state.tableRows.length} row(s) · ${scopeLabel}`;
  els.editorTable.innerHTML = "";
  els.tableEmpty.classList.toggle("hidden", state.tableRows.length > 0);

  const thead = document.createElement("thead");
  const headRow = document.createElement("tr");
  for (const column of columns) {
    const th = document.createElement("th");
    th.textContent = column.name;
    headRow.append(th);
  }
  const actionsTh = document.createElement("th");
  actionsTh.textContent = "Actions";
  headRow.append(actionsTh);
  thead.append(headRow);
  els.editorTable.append(thead);

  const tbody = document.createElement("tbody");
  for (const [index, row] of state.tableRows.entries()) {
    tbody.append(buildTableRow(row, state.tableAnnotations[index] || {}));
  }
  els.editorTable.append(tbody);
}

function buildTableRow(row, annotations) {
  const tr = document.createElement("tr");
  for (const column of state.tableSchema.columns) {
    const td = document.createElement("td");
    td.className = "cell";
    const stack = document.createElement("div");
    stack.className = "cell-stack";
    const control = buildEditorControl(column, row);
    stack.append(control);

    const annotation = annotations[column.name];
    if (annotation) {
      const note = document.createElement("div");
      note.className = "cell-note";
      note.textContent = annotation.label;
      stack.append(note);

      if (annotation.detail) {
        const detail = document.createElement("div");
        detail.className = "cell-detail";
        detail.textContent = annotation.detail;
        stack.append(detail);
      }
    }

    td.append(stack);
    tr.append(td);
  }

  const actions = document.createElement("td");
  actions.className = "actions";

  const save = document.createElement("button");
  save.className = "primary-button";
  save.textContent = "Save";
  save.addEventListener("click", async () => {
    const payload = {};
    for (const control of tr.querySelectorAll("input, select")) {
      payload[control.dataset.column] = control.value;
    }
    const params = new URLSearchParams();
    if (state.selectedUserId && currentTableIsUserScoped()) {
      params.set("user_id", state.selectedUserId);
    }
    const suffix = params.toString() ? `?${params.toString()}` : "";
    await fetchJSON(`/api/table/${state.selectedTable}${suffix}`, {
      method: "POST",
      body: JSON.stringify({ row: payload }),
    });
    await loadTable(state.selectedTable);
  });

  const remove = document.createElement("button");
  remove.className = "danger-button";
  remove.textContent = "Delete";
  remove.addEventListener("click", async () => {
    const key = {};
    for (const name of state.tableSchema.primaryKey) {
      const control = tr.querySelector(`[data-column="${name}"]`);
      key[name] = control?.value ?? "";
    }
    const params = new URLSearchParams();
    if (state.selectedUserId && currentTableIsUserScoped()) {
      params.set("user_id", state.selectedUserId);
    }
    const suffix = params.toString() ? `?${params.toString()}` : "";
    await fetchJSON(`/api/table/${state.selectedTable}${suffix}`, {
      method: "DELETE",
      body: JSON.stringify({ key }),
    });
    await loadTable(state.selectedTable);
  });

  actions.append(save, remove);
  tr.append(actions);
  return tr;
}

async function loadTable(table) {
  state.selectedTable = table;
  const params = new URLSearchParams();
  if (state.selectedUserId && currentTableIsUserScoped()) {
    params.set("user_id", state.selectedUserId);
  }
  const suffix = params.toString() ? `?${params.toString()}` : "";
  const data = await fetchJSON(`/api/table/${table}${suffix}`);
  state.tableRows = data.rows;
  state.tableAnnotations = data.annotations || [];
  state.tableSchema = data.schema;
  if (state.tableSchema.columns.some((column) => column.name === "character_id")) {
    await ensureLookupOptions("character_id");
  }
  renderTable();
}

async function selectUser(userId) {
  const user = await fetchJSON(`/api/user/${userId}/summary`);
  renderUserSummary(user);
  renderUsers();
  await loadTable(state.selectedTable || els.tableSelect.value);
}

async function initialize() {
  state.overview = await fetchJSON("/api/overview");
  state.users = state.overview.users || [];
  renderOverview();
  populateTableSelect();
  renderUsers();

  if (state.users.length > 0) {
    await selectUser(state.users[0].userId);
  } else if (state.selectedTable) {
    await loadTable(state.selectedTable);
  }
}

async function refreshOverviewAndSelection() {
  state.overview = await fetchJSON("/api/overview");
  state.users = state.overview.users || [];
  renderOverview();
  populateTableSelect();
  renderUsers();

  if (state.users.length === 0) {
    state.selectedUserId = null;
    els.userSummaryPanel.classList.add("hidden");
    els.tablePanel.classList.remove("hidden");
    els.heroTitle.textContent = "No users in this database";
    els.heroSubtitle.textContent = "Global and system tables are still available to inspect and edit.";
    if (state.selectedTable) {
      await loadTable(state.selectedTable);
    }
    return;
  }

  const remaining = state.users.find((user) => String(user.userId) === String(state.selectedUserId));
  const fallback = remaining || state.users[0];
  await selectUser(fallback.userId);
}

els.refreshUsers.addEventListener("click", async () => {
  await refreshOverviewAndSelection();
});

els.tableSearch.addEventListener("input", async (event) => {
  state.tableSearch = event.target.value;
  populateTableSelect();
  if (state.selectedTable) {
    await loadTable(state.selectedTable);
  } else {
    state.tableRows = [];
    state.tableAnnotations = [];
    state.tableSchema = null;
    els.editorTable.innerHTML = "";
    els.tableMeta.textContent = "No matching tables.";
    els.tableEmpty.classList.remove("hidden");
  }
});

els.tableSelect.addEventListener("change", async (event) => {
  state.selectedTable = event.target.value;
  if (state.selectedUserId || !currentTableIsUserScoped()) {
    await loadTable(state.selectedTable);
  }
});

els.addRow.addEventListener("click", () => {
  if (!state.tableSchema) return;
  state.tableRows = [buildBlankRow(), ...state.tableRows];
  renderTable();
});

els.deleteUser.addEventListener("click", async () => {
  if (!state.selectedUserId) return;
  const confirmed = window.confirm(
    `Delete user ${state.selectedUserId} and all linked rows from the database? This cannot be undone.`
  );
  if (!confirmed) return;

  await fetchJSON(`/api/user/${state.selectedUserId}`, {
    method: "DELETE",
  });
  await refreshOverviewAndSelection();
});

initialize().catch((error) => {
  els.heroTitle.textContent = "Failed to load editor";
  els.heroSubtitle.textContent = error.message;
  console.error(error);
});
