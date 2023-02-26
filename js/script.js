function tableFixHead(e) {
  const el = e.target, sT = el.scrollTop;

  el.querySelectorAll("th").forEach(th =>
    th.style.transform = `translateY(${sT}px)`
  );
}
document.querySelectorAll(".table").forEach(el =>
  el.addEventListener("scroll", tableFixHead)
);

document.addEventListener('alpine:init', () => {

  function sendPostRequest(data) {

    // strip out '/' from the url
    let curr_table_id = window.location.pathname.split('/')[2];

    return fetch(`/tables/${curr_table_id}`, {
      method: "POST",
      body: JSON.stringify(data),
      headers: {
        'Content-Type': 'application/json'
      },
    })
  }

  function openTable(table_id) {
    // opens new url corresponding to given table_id
    window.location.href = new URL(`tables/${table_id}`, window.location.origin)
  }

  function openNextPage() {
    document.getElementById('next-page').click()
  }
  function openPrevPage() {
    document.getElementById('previous-page').click()
  }
  Alpine.store('main', {
    activeRowOffset: -1,
  })

  Alpine.data('table_parent', () => ({
    loading: false,
    filter_vals: {}, // contains { column_index: [val1, val2], ... }
    key_cols: [], // contains indices
    search_mode: false,

    init() {
      this.loading = false
    },

  }))
  Alpine.bind('table_parent_bind', () => ({
    ':class'() {
      // when loading, makes table grey
      return this.loading ? 'loading-table' : ''
    }
  }))

  Alpine.bind('progress', () => ({
    ':class'() {
      // when not loading, hides the progress bar
      return { 'hidden': !this.loading }
    }
  }))

  Alpine.data('table', (num_rows, num_cols, parent_table_id, wrapped_col_indices) => ({
    rowidx: 0,
    colidx: 0,
    hidden_cols: new Set(), // contains indices of hidden columns
    agg_col: undefined,
    search_input: '',
    // contains indices of columns that are rendered over multiple lines
    col_wrapping: Object.fromEntries([...Array(num_cols).keys()].map(x => [x, wrapped_col_indices.includes(x) ? 'wrap' : 'clip'])),

    saveStateToStorage(key = window.location.pathname) {
      // localStorage.setItem(window.location.pathname, JSON.stringify({rowidx: this.rowidx, colidx: this.colidx}))
      sessionStorage.setItem(
        key, JSON.stringify(
          {
            rowidx: this.rowidx,
            colidx: this.colidx,
            hidden_cols: Array.from(this.hidden_cols),
            col_wrapping: this.col_wrapping,
          })
      )
    },

    init() {
      let state = sessionStorage.getItem(window.location.pathname)
      if ("last" in sessionStorage) {
        state = sessionStorage["last"]
        sessionStorage.removeItem("last")
      }
      if (state) {
        state = JSON.parse(state)
        this.rowidx = state.rowidx
        this.colidx = state.colidx
        this.hidden_cols = new Set(state.hidden_cols)
        if (state.col_wrapping) {
          this.col_wrapping = state.col_wrapping
        }
      }
    },

    performOp(op, args) {
      // set `loading` to true before sending post request
      this.loading = true;

      // on google chrome, after a regex operation, if user
      // goes back, the search mode is still active but the
      // search input is not in focus, which breaks seamlessness of
      // the UI. So, we disable search mode beforehand
      this.disable_search_mode()

      // sorting changes the url, but it shouldn't change state
      // the 'last' key is a way of passing current state to next page
      if (op == "sa" || op == "sd") {
        this.saveStateToStorage("last")
      }

      let data = { operation_type: op, ...args };
      const promise = sendPostRequest(data);

      promise.then(response => {
        // alert the user if response is 400
        if (response.status == 400) {
          response.json().then(body => {
            this.loading = false
            alert(body.detail)
          })
        }
        else {
          response.json().then(body => {
            console.log(body);
            if (body.new_table != undefined) {
              this.loading = false
              openTable(body.new_table)
            }
          });
        }
      });
    },

    performFilterOp() {
      // filter
      const filters = [] // [(col, val), ...]
      const cols_to_return = this.get_visible_col_names()

      for (const colidx in this.filter_vals) {
        this.filter_vals[colidx].forEach(value => filters.push([this.$refs[`col-${colidx}`].getAttribute("data-colname"), value]))
      }
      if (filters.length == 0 && this.hidden_cols.size == 0) {
        alert("pick some values or columns to filter on")
        return
      }
      this.performOp("fil", { 'filters': filters, 'columns_to_return': cols_to_return, criterion: "any" });
    },

    performPivotOp() {
      if (this.agg_col == undefined) {
        alert("pick a column to aggregate on")
        return
      }
      // this logic is repeating
      const key_col_names = this.key_cols.map(colidx => this.$refs[`col-${colidx}`].getAttribute("data-colname"));
      const pivot_col = this.$refs[`col-${this.colidx}`].getAttribute("data-colname");
      const agg_col = this.$refs[`col-${this.agg_col}`].getAttribute("data-colname");
      this.performOp("pivot", { 'key_cols': key_col_names, 'pivot_col': pivot_col, 'agg_col': agg_col });
    },

    performMultiFrequencyOp() {
      const col_names = this.key_cols.map(colidx => this.$refs[`col-${colidx}`].getAttribute("data-colname"));
      this.performOp("f", { 'cols': col_names });
    },

    performFrequencyOp() {
      const col_name = this.$refs[`col-${this.colidx}`].getAttribute("data-colname");
      this.performOp("f", { 'cols': [col_name] });
    },

    performFacetOp(key_cols) {

      // key_cols is a list of column indices
      const filters = key_cols.map(j => [
        this.$refs[`col-${j}`].getAttribute("data-colname"),
        cellToVal(this.$refs[`cell-${this.rowidx}-${j}`])
      ])

      this.performOp("fac", { 'facets': filters });
    },

    performOpenOp() {
      this.performOp("open", { 'rowid': this.rowidx });
    },

    performSortOp(type) {
      // type is one of 'sa', 'sd'
      const col_name = this.$refs[`col-${this.colidx}`].getAttribute("data-colname");
      this.performOp(type, { 'params': col_name })
    },

    performRegexSearchOp() {
      const cols_to_return = this.get_visible_col_names()
      const col_name = this.$refs[`col-${this.colidx}`].getAttribute("data-colname");
      this.performOp("search", {
        'col': col_name,
        'regex': this.search_input,
        'columns_to_return': cols_to_return
      })
    },

    update_rowid(delta) {
      this.rowidx = Math.max(Math.min(this.rowidx + delta, num_rows - 1), 0)
    },
    update_colid(delta) {
      this.colidx = Math.max(Math.min(this.colidx + delta, num_cols - 1), 0)
    },
    update_rowid_to_max() {
      this.rowidx = num_rows - 1;
    },
    update_rowid_to_min() {
      this.rowidx = 0;
    },
    toggle_key(colidx) {
      let index = this.key_cols.indexOf(colidx);

      if (index === -1) {
        this.key_cols.push(colidx);
      } else {
        this.key_cols.splice(index, 1);
      }
    },
    toggle_agg_col(colidx) {
      if (colidx == this.agg_col) {
        this.agg_col = undefined
      } else {
        this.agg_col = colidx
      }
    },

    toggle_filter_vals() {
      const value = cellToVal(this.$refs[`cell-${this.rowidx}-${this.colidx}`])
      const colidx = this.colidx

      // toggles the presence of (colidx, value) in filter_vals
      if (colidx in this.filter_vals) {
        const vals = this.filter_vals[colidx]
        if (vals.has(value)) {
          vals.delete(value)
          if (vals.size == 0) {
            delete this.filter_vals[colidx]
          }
        } else {
          vals.add(value)
        }
      } else {
        this.filter_vals[colidx] = new Set([value])
      }
    },

    toggle_hidden_col() {
      if (this.hidden_cols.has(this.colidx)) {
        this.hidden_cols.delete(this.colidx)
      } else {
        this.hidden_cols.add(this.colidx)
      }
      this.update_colid_to_next_visible()
    },

    get_visible_col_names() {
      const nodelist = document.querySelectorAll("table th");
      const colnames = Array.from(nodelist).map(node => node.getAttribute("data-colname"))
      return colnames.filter((_, i) => !this.hidden_cols.has(i))
    },

    update_colid_to_next_visible() {
      // gets indices of all visible columns
      const visible_col_indices = [...Array(num_cols).keys()].filter(i => !this.hidden_cols.has(i))

      // [(index, distance from active col), ...]
      // the 0.5 gives right columns preference
      const deltas = visible_col_indices.map(i => [i, Math.abs(i - this.colidx - 0.5)]).sort((a, b) => a[1] - b[1])

      if (deltas.length != 0) {
        this.colidx = deltas[0][0]
      }
    },

    scrollCellIntoView(i, j) {
      // scrolls a cell with index (i,j) into view if its located near the
      // active cell
      const is_near_active_row = (this.rowidx <= i + 3 && this.rowidx >= i - 2)
      const is_near_active_col = this.colidx == j

      if (is_near_active_row && is_near_active_col) {
        this.$refs[`cell-${i}-${j}`].scrollIntoView({
          block: this.rowidx == 0 ? 'end' : 'nearest',
          inline: 'nearest'
        })
      }

      // scrolling behavior is a little tricky due to the floating header
      // this code block is a hacky way to fix that behavior when scrolling
      // from down to up
      const is_near_header = this.rowidx <= 2
      if (is_near_header) {
        this.$refs['header'].scrollIntoView({
          block: this.rowidx == 0 ? 'end' : 'nearest',
          inline: 'nearest'
        })
      }
    },

    toggle_multiline_col() {
      if (this.col_wrapping[this.colidx] == 'clip') {
        this.col_wrapping[this.colidx] = 'wrap'
      } else if (this.col_wrapping[this.colidx] == 'wrap') {
        this.col_wrapping[this.colidx] = ''
      } else {
        this.col_wrapping[this.colidx] = 'clip'
      }
    },

    is_search_match(rowidx) {
      let re;
      try {
        re = new RegExp(this.search_input)
      } catch (e) {
        return false
      }

      const val = cellToVal(this.$refs[`cell-${rowidx}-${this.colidx}`])
      if (val == null) return false;

      return val.search(re) >= 0
    },

    is_filtered(rowidx) {
      const row_el = this.$refs[`row-${rowidx}`]
      // if `filter_vals` is empty return false
      if (Object.keys(this.filter_vals).length == 0) {
        return false
      }

      for (var j = 0, cell; cell = row_el.cells[j]; j++) {
        if (j in this.filter_vals && this.filter_vals[j].has(cellToVal(cell))) {
          return false
        }
      }
      return true
    },

    enable_search_mode(e) {
      // prevents the '|' from appearing in input box
      if (!this.search_mode) { e.preventDefault() }
      this.search_mode = true;
    },

    disable_search_mode() {
      this.search_mode = false;
      this.search_input = '';
    },

    goback() {
      // openTable(parent_table_id)
      history.back()
    },

    goforward() {
      history.forward()
    },

    handleKeydown(e) {
      // console.log(e.key);
      if (e.ctrlKey) {
        return
      }
      const shift_key_map = {
        'G': () => { this.update_rowid_to_max() },
        'F': () => { this.performFrequencyOp() },
        'W': () => { this.performPivotOp() },
        'N': openNextPage,
        'P': openPrevPage,
      }
      const key_map = {
        'g': () => { this.update_rowid_to_min() },
        'f': () => { this.performMultiFrequencyOp() },
        'j': () => { this.update_rowid(1) },
        'k': () => { this.update_rowid(-1) },
        'l': () => { this.update_colid(1) },
        'h': () => { this.update_colid(-1) },
        'ArrowDown': () => { this.update_rowid(1) },
        'ArrowUp': () => { this.update_rowid(-1) },
        'ArrowRight': () => { this.update_colid(1) },
        'ArrowLeft': () => { this.update_colid(-1) },
        '!': () => { this.toggle_key(this.colidx) },
        '+': () => { this.toggle_agg_col(this.colidx) },
        ',': () => { this.toggle_filter_vals() },
        '-': () => { this.toggle_hidden_col() },
        '"': () => { this.performFilterOp() },
        '[': () => { this.performSortOp('sa') },
        ']': () => { this.performSortOp('sd') },
        '|': () => { this.enable_search_mode(e) },
        'q': () => { this.goback() },
        'p': () => { this.goforward() },
        'v': () => { this.toggle_multiline_col() },
      }
      const searchmode_key_map = {
        'Escape': () => { this.disable_search_mode() }
      }
      if (this.search_mode) {
        if (e.key in searchmode_key_map) searchmode_key_map[e.key]();
        console.log(e.key)
        return
      }

      if (e.shiftKey & e.key in shift_key_map) {
        shift_key_map[e.key]()
      }
      else if (e.key in key_map) {
        key_map[e.key]()
      }
    }

  }));

  let base_bindings = () => ({

    "@keydown.window"(e) { this.handleKeydown(e) },

    // state is stored on every keydown event
    // after a delay of 250ms
    "@keydown.window.debounce"() {
      this.saveStateToStorage()
    }
  });

  Alpine.bind('base_table', () => ({
    ...base_bindings(),

    '@keydown.enter.window'() {
      if (this.search_mode || this.loading) {
        return
      }
      this.performOpenOp()
    },

  }));

  function cellToVal(cell_el) {
    // check if cell is NULL or an empty string
    if (cell_el.classList.contains("null")) {
      return null
    } else {
      return cell_el.getAttribute('data-val');
    }
  }

  Alpine.bind('freq_table', (key_cols) => ({
    ...base_bindings(),

    '@keydown.enter.window'() {
      if (this.search_mode || this.loading) { return }
      this.performFacetOp(key_cols)
    },

  }));

  Alpine.bind('row', (idx) => ({
    ':class'() {
      return {
        'active': this.rowidx == idx,
        'filtered': (!this.search_mode & this.is_filtered(idx)) | (this.search_mode & !this.is_search_match(idx)),
      }
    },

    'x-init'() {
      this.$nextTick(() => {
        if (this.rowidx == idx) {

          this.$store.main.activeRowOffset = window.pageXOffset + this.$el.getBoundingClientRect().top
        }
      })
    },

  }));

  Alpine.bind('col', (j) => ({
    ':class'() {
      return {
        'active': (this.colidx == j),
        'key-col': this.key_cols.includes(j),
        'agg-col': this.agg_col == j,
        'hidden-col': this.hidden_cols.has(j),
      }
    }

  }));

  Alpine.bind('cell', (i, j) => ({

    ':class'() {
      return {
        'filtered-val': (j in this.filter_vals) && (this.filter_vals[j].has(cellToVal(this.$refs[`cell-${i}-${j}`]))),
        // this is commented out due to performance reasons
        // will try to find another approach later
        // 'selected-cell': (this.rowidx == i) && (this.colidx == j),
        'selected-col': (this.colidx == j),
        'hidden-cell': this.hidden_cols.has(j),
        'clipped-cell': this.col_wrapping[j] == 'clip',
        'wrapped-cell': this.col_wrapping[j] == 'wrap',
      }
    },

    'x-effect'() {
      // watching `col_wrapping` for changes
      // scrolls if it changes
      this.col_wrapping[j] == 'clip';
      this.col_wrapping[j] == 'wrap';

      this.scrollCellIntoView(i, j);
      const activeRow = this.$refs[`row-${this.rowidx}`]
      this.$store.main.activeRowOffset = window.pageXOffset + activeRow.getBoundingClientRect().top
    },

    '@click'() {
      this.rowidx = i
      this.colidx = j
    },

  }));

  Alpine.bind('search_row', () => ({
    '@click.outside'() {
      this.disable_search_mode()
    },

    ':style'() {
      return {
        'display': this.search_mode ? '' : 'none'
      }
    }
  }));

  Alpine.bind('search_div', (colidx) => ({
    ':style'() {
      return {
        'display': this.search_mode & (colidx == this.colidx) ? '' : 'none'
      }
    }
  }));

  Alpine.bind('search_input', (colidx) => ({
    'x-effect'() {
      if (this.search_mode & colidx == this.colidx) {
        this.$el.focus()
      }
    },

    // Note: all input elements (there's one for each col) are
    // bound to the same variable `this.search_input`
    'x-model': 'search_input',

    '@keydown.enter'() {
      this.performRegexSearchOp()
    },

    ':class'() {
      try {
        new RegExp(this.search_input)
      } catch (e) {
        return 'is-error'
      }
      return 'is-success'
    }
  }));

  Alpine.bind('search_close_btn', () => ({
    '@click'() {
      this.disable_search_mode()
    }
  }));

  Alpine.bind('hints_sidebar', () => ({
    'x-show'() {
      return this.$store.main.activeRowOffset > 0
    },

    ':style'() {

      return {
        'transform': `translateY(${this.$store.main.activeRowOffset - 50}px)`
      }
    },
  }));

  Alpine.bind('open_hint', () => ({
    'x-show'() {

      const filters_active = Object.keys(this.filter_vals).length !== 0
      return !this.search_mode && !filters_active
    },
    'x-on:click'() {
      window.dispatchEvent(new KeyboardEvent('keydown', { 'key': 'enter' }));
    }
  }));

  Alpine.bind('filter_hint', () => ({
    'x-show'() {
      const filters_active = Object.keys(this.filter_vals).length !== 0
      return filters_active && !this.search_mode
    },

    'x-on:click'() {
      window.dispatchEvent(new KeyboardEvent('keydown', { 'key': '"' }));
    }
  }));

  Alpine.bind('freq_hint', () => ({
    'x-show'() {
      const key_cols_selected = Object.keys(this.key_cols).length !== 0
      return key_cols_selected && !this.search_mode
    },

    'x-on:click'() {
      window.dispatchEvent(new KeyboardEvent('keydown', { 'key': '"' }));
    }
  }));
});
