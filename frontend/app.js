// Alpine.js application state and logic
document.addEventListener('alpine:init', () => {
  Alpine.data('app', () => ({
    // ── state ────────────────────────────────────────────────────────────────
    fileId: null,
    fileName: '',
    sheets: [],
    selectedSheet: '',
    columns: [],

    steps: [],

    // 整形後プレビュー
    previewColumns: [],
    previewRows: [],
    totalRows: 0,

    // 元ファイルプレビュー（アップロード時に保存）
    originalColumns: [],
    originalRows: [],
    originalTotalRows: 0,

    // 参照ファイルプレビュー（left join用）
    refFileId: null,
    refFileName: '',
    refSheets: [],
    refSelectedSheet: '',
    refColumns: [],
    refRows: [],
    refTotalRows: 0,

    activeTab: 'original', // 'original' | 'transformed' | 'reference'

    loading: false,
    previewLoading: false,
    debounceTimer: null,

    toasts: [],
    nextToastId: 0,

    stepTypes: [
      { value: 'delete_rows',     label: '行削除' },
      { value: 'delete_columns',  label: '列削除' },
      { value: 'unpivot',         label: 'アンピボット (melt)' },
      { value: 'left_join',       label: 'Left Join' },
      { value: 'filter_rows',     label: '行フィルター' },
      { value: 'reorder_columns', label: '列並び替え' },
    ],

    selectedStepType: 'filter_rows',

    operators: [
      { value: '==',           label: '= (等しい)' },
      { value: '!=',           label: '≠ (等しくない)' },
      { value: '>',            label: '> (より大きい)' },
      { value: '<',            label: '< (より小さい)' },
      { value: '>=',           label: '≥ (以上)' },
      { value: '<=',           label: '≤ (以下)' },
      { value: 'contains',     label: '含む' },
      { value: 'not_contains', label: '含まない' },
      { value: 'starts_with',  label: '始まる' },
      { value: 'ends_with',    label: '終わる' },
      { value: 'is_null',      label: 'NULLである' },
      { value: 'is_not_null',  label: 'NULLでない' },
    ],

    // ── upload ───────────────────────────────────────────────────────────────
    async handleFileDrop(event) {
      event.preventDefault();
      const file = event.dataTransfer?.files?.[0];
      if (file) await this.uploadFile(file);
    },

    async handleFileInput(event) {
      const file = event.target.files?.[0];
      if (file) await this.uploadFile(file);
    },

    async uploadFile(file) {
      this.loading = true;
      try {
        const form = new FormData();
        form.append('file', file);
        const res = await fetch('/upload', { method: 'POST', body: form });
        const data = await this._handleResponse(res);
        this.fileId = data.file_id;
        this.fileName = file.name;
        this.sheets = data.sheets;
        this.selectedSheet = data.selected_sheet;
        this.columns = data.columns;
        this.originalColumns = data.columns;
        this.originalRows = data.rows;
        this.originalTotalRows = data.total_rows;
        this.steps = [];
        this.activeTab = 'original';
        await this.applyDefaultPipeline();
        this.showToast('ファイルを読み込みました', 'success');
      } catch (e) {
        this.showToast(e.message, 'error');
      } finally {
        this.loading = false;
      }
    },

    async selectSheet() {
      if (!this.fileId) return;
      this.loading = true;
      try {
        const form = new FormData();
        form.append('file_id', this.fileId);
        form.append('sheet_name', this.selectedSheet);
        const res = await fetch('/select-sheet', { method: 'POST', body: form });
        const data = await this._handleResponse(res);
        this.columns = data.columns;
        this.originalColumns = data.columns;
        this.originalRows = data.rows;
        this.originalTotalRows = data.total_rows;
        this.steps = [];
        this.activeTab = 'original';
        await this.applyDefaultPipeline();
      } catch (e) {
        this.showToast(e.message, 'error');
      } finally {
        this.loading = false;
      }
    },

    clearFile() {
      if (this.fileId) {
        fetch(`/session/${this.fileId}`, { method: 'DELETE' }).catch(() => {});
      }
      this.fileId = null;
      this.fileName = '';
      this.sheets = [];
      this.selectedSheet = '';
      this.columns = [];
      this.steps = [];
      this.originalColumns = [];
      this.originalRows = [];
      this.originalTotalRows = 0;
      this.previewColumns = [];
      this.previewRows = [];
      this.totalRows = 0;
      this.activeTab = 'original';
    },

    // ── 参照ファイル ──────────────────────────────────────────────────────────
    async handleRefFileInput(event) {
      const file = event.target.files?.[0];
      if (file) await this.uploadRefFile(file);
    },

    async handleRefFileDrop(event) {
      event.preventDefault();
      const file = event.dataTransfer?.files?.[0];
      if (file) await this.uploadRefFile(file);
    },

    async uploadRefFile(file) {
      this.loading = true;
      try {
        const form = new FormData();
        form.append('file', file);
        if (this.refSelectedSheet) form.append('sheet_name', this.refSelectedSheet);
        const res = await fetch('/upload-join-file', { method: 'POST', body: form });
        const data = await this._handleResponse(res);
        this.refFileId = data.file_id;
        this.refFileName = file.name;
        this.refSheets = data.sheets;
        this.refSelectedSheet = data.selected_sheet;
        this.refColumns = data.columns;
        // Step2のleft_joinステップに自動反映
        const joinStep = this.steps.find(s => s.type === 'left_join' && !s.join_file_id);
        if (joinStep) {
          joinStep.join_file_id = data.file_id;
          joinStep.join_file_name = file.name;
          joinStep.join_sheets = data.sheets;
          joinStep.join_columns = data.columns;
          this.schedulePreview();
        }
        // プレビュー取得
        await this.fetchRefPreview();
        this.showToast('参照ファイルを読み込みました', 'success');
      } catch (e) {
        this.showToast(e.message, 'error');
      } finally {
        this.loading = false;
      }
    },

    async fetchRefPreview() {
      if (!this.refFileId) return;
      try {
        const res = await fetch('/preview', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ file_id: this.refFileId, steps: [] }),
        });
        const data = await this._handleResponse(res);
        this.refColumns = data.columns;
        this.refRows = data.rows;
        this.refTotalRows = data.total_rows;
      } catch (e) {
        this.showToast(e.message, 'error');
      }
    },

    async selectRefSheet() {
      if (!this.refFileId) return;
      this.loading = true;
      try {
        const form = new FormData();
        form.append('file_id', this.refFileId);
        form.append('sheet_name', this.refSelectedSheet);
        const res = await fetch('/select-sheet', { method: 'POST', body: form });
        const data = await this._handleResponse(res);
        this.refColumns = data.columns;
        this.refRows = data.rows;
        this.refTotalRows = data.total_rows;
      } catch (e) {
        this.showToast(e.message, 'error');
      } finally {
        this.loading = false;
      }
    },

    clearRefFile() {
      if (this.refFileId) {
        const oldRefFileId = this.refFileId;
        fetch(`/session/${oldRefFileId}`, { method: 'DELETE' }).catch(() => {});
        // Step2のleft_joinステップのjoin_file_idをクリア
        this.steps.forEach(s => {
          if (s.type === 'left_join' && s.join_file_id === oldRefFileId) {
            s.join_file_id = '';
            s.join_file_name = '';
            s.join_columns = [];
          }
        });
        this.schedulePreview();
      }
      this.refFileId = null;
      this.refFileName = '';
      this.refSheets = [];
      this.refSelectedSheet = '';
      this.refColumns = [];
      this.refRows = [];
      this.refTotalRows = 0;
    },

    // ── default pipeline (convert.py と同期) ─────────────────────────────────
    async applyDefaultPipeline() {
      // Step1: 1列目を固定してアンピボット
      const idCol = this.columns[0];
      if (!idCol) return;
      this.steps = [
        {
          type: 'unpivot',
          id_vars: [idCol],
          value_vars: [],      // 空 = 残り全列
          var_name: '日付',
          value_name: 'シフト',
          _open: false,
        },
        // Step2: シフト対応表をleft join（シフト列 → シフト名、パターンコードを取得）
        {
          type: 'left_join',
          join_file_id: this.refFileId || '',
          join_file_name: this.refFileName || '',
          join_sheets: this.refSheets || [],
          join_columns: this.refColumns || [],
          left_on: 'シフト',
          right_on: 'シフト名',
          select_cols: ['パターンコード'],
          _open: false,
        },
      ];
      await this.fetchPreview();
    },

    // ── steps management ─────────────────────────────────────────────────────
    addStep() {
      const type = this.selectedStepType;
      const step = this._defaultStep(type);
      this.steps.push(step);
      this.activeTab = 'transformed';
      this.schedulePreview();
    },

    removeStep(index) {
      this.steps.splice(index, 1);
      this.schedulePreview();
    },

    _defaultStep(type) {
      const base = { type, _open: true };
      if (type === 'delete_rows')     return { ...base, mode: 'index_range', start: 0, end: 1, column: '', operator: '==', value: '' };
      if (type === 'delete_columns')  return { ...base, columns: [] };
      if (type === 'unpivot')         return { ...base, id_vars: [], value_vars: [], var_name: 'variable', value_name: 'value' };
      if (type === 'left_join')       return { ...base, join_file_id: '', join_file_name: '', join_sheets: [], join_columns: [], left_on: '', right_on: '', select_cols: [] };
      if (type === 'filter_rows')     return { ...base, column: this.columns[0] || '', operator: '==', value: '' };
      if (type === 'reorder_columns') return { ...base, order: [...this.previewColumns] };
      return base;
    },

    stepLabel(type) {
      return this.stepTypes.find(s => s.value === type)?.label ?? type;
    },

    // ── column toggle helpers ─────────────────────────────────────────────────
    toggleColumn(step, col) {
      const idx = step.columns.indexOf(col);
      if (idx === -1) step.columns.push(col);
      else step.columns.splice(idx, 1);
      this.schedulePreview();
    },

    toggleIdVar(step, col) {
      const idx = step.id_vars.indexOf(col);
      if (idx === -1) step.id_vars.push(col);
      else step.id_vars.splice(idx, 1);
      this.schedulePreview();
    },

    toggleValueVar(step, col) {
      const idx = step.value_vars.indexOf(col);
      if (idx === -1) step.value_vars.push(col);
      else step.value_vars.splice(idx, 1);
      this.schedulePreview();
    },

    // ── join file ─────────────────────────────────────────────────────────────
    async uploadJoinFile(step, event) {
      const file = event.target.files?.[0];
      if (!file) return;
      try {
        const form = new FormData();
        form.append('file', file);
        const res = await fetch('/upload-join-file', { method: 'POST', body: form });
        const data = await this._handleResponse(res);
        step.join_file_id = data.file_id;
        step.join_file_name = file.name;
        step.join_sheets = data.sheets;
        step.join_columns = data.columns;
        if (!step.right_on && data.columns.length) step.right_on = data.columns[0];
        this.schedulePreview();
      } catch (e) {
        this.showToast(e.message, 'error');
      }
    },

    // ── reorder columns sortable ──────────────────────────────────────────────
    initSortable(stepIndex) {
      this.$nextTick(() => {
        const el = document.getElementById(`sortable-${stepIndex}`);
        if (!el || el._sortable) return;
        const step = this.steps[stepIndex];
        el._sortable = Sortable.create(el, {
          animation: 150,
          ghostClass: 'sortable-ghost',
          handle: '.drag-handle',
          onEnd: (evt) => {
            const moved = step.order.splice(evt.oldIndex, 1)[0];
            step.order.splice(evt.newIndex, 0, moved);
            this.schedulePreview();
          },
        });
      });
    },

    syncReorderColumns(step) {
      // Add any new columns from current preview that aren't in order yet
      this.previewColumns.forEach(c => {
        if (!step.order.includes(c)) step.order.push(c);
      });
      // Remove columns no longer present
      step.order = step.order.filter(c => this.previewColumns.includes(c));
    },

    // ── preview ───────────────────────────────────────────────────────────────
    schedulePreview() {
      clearTimeout(this.debounceTimer);
      this.debounceTimer = setTimeout(() => this.fetchPreview(), 300);
    },

    async fetchPreview() {
      if (!this.fileId) return;
      this.previewLoading = true;
      try {
        const res = await fetch('/preview', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify(this._buildRequest()),
        });
        const data = await this._handleResponse(res);
        this.previewColumns = data.columns;
        this.previewRows = data.rows;
        this.totalRows = data.total_rows;
      } catch (e) {
        this.showToast(e.message, 'error');
      } finally {
        this.previewLoading = false;
      }
    },

    // ── export ────────────────────────────────────────────────────────────────
    async exportCSV() {
      if (!this.fileId) return;
      this.loading = true;
      try {
        const res = await fetch('/run', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify(this._buildRequest()),
        });
        if (!res.ok) {
          const err = await res.json().catch(() => ({ detail: res.statusText }));
          throw new Error(err.detail || res.statusText);
        }
        const blob = await res.blob();
        const url = URL.createObjectURL(blob);
        const a = document.createElement('a');
        a.href = url;
        a.download = 'output.csv';
        a.click();
        URL.revokeObjectURL(url);
        this.showToast('CSVを書き出しました', 'success');
      } catch (e) {
        this.showToast(e.message, 'error');
      } finally {
        this.loading = false;
      }
    },

    // ── helpers ───────────────────────────────────────────────────────────────
    _buildRequest() {
      return {
        file_id: this.fileId,
        sheet_name: this.selectedSheet,
        steps: this.steps.map(s => {
          // strip UI-only fields
          const { _open, join_file_name, join_sheets, join_columns, ...rest } = s;
          return rest;
        }),
      };
    },

    async _handleResponse(res) {
      if (!res.ok) {
        const err = await res.json().catch(() => ({ detail: res.statusText }));
        throw new Error(err.detail || res.statusText);
      }
      return res.json();
    },

    // ── toasts ────────────────────────────────────────────────────────────────
    showToast(message, type = 'info') {
      const id = this.nextToastId++;
      this.toasts.push({ id, message, type });
      setTimeout(() => {
        this.toasts = this.toasts.filter(t => t.id !== id);
      }, 4000);
    },

    // ── drag over helper ──────────────────────────────────────────────────────
    dragOver(event) { event.preventDefault(); },
  }));
});
