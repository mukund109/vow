document.addEventListener('alpine:init', () => {

  function sendPostRequest(data) {

    // strip out '/' from the url
    let path = window.location.pathname.replace('\/', '');

    return fetch(`/sheets/${path}`, {
      method: "POST",
      body: JSON.stringify(data),
      headers: {
        'Content-Type': 'application/json'
      },
    })
  }

  function openSheet(sheet_id) {
    // opens new url corresponding to given sheet_id
    window.location.href = new URL(sheet_id, window.location.origin)
  }

  Alpine.data('sheet_parent', () => ({ loading: false }))
  Alpine.bind('sheet_parent_bind', () => ({
    ':class'() {
      // when loading, makes table grey
      return this.loading ? 'loading-table' : ''
    }
  }))
  Alpine.bind('progress', () => ({
    ':class'() {
      // when not loading, hides the progress bar
      return { 'hidden' : !this.loading }
    }
  }))

  Alpine.data('sheet', (num_rows, num_cols) => ({
    rowidx: 0,
    colidx: 0,
    key_cols: [], // contains indices
    filter_vals: {}, // contains { column_index: value, ... }
    agg_col: undefined,

    performOp(op, args) {
      // set `loading` to true before sending post request
      this.loading = true;

      let data = { operation_type: op, ...args };
      const promise = sendPostRequest(data);

      promise.then(response => {
        // alert the user if response is 400
        if (response.status == 400){
          response.json().then(body => {
            this.loading = false
            alert(body.detail)
          })
        }
        else{
          response.json().then(body => {
            console.log(body);
            if (body.new_sheet != undefined) {
              openSheet(body.new_sheet)
            }
          });
        }
      });
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
    }

  }));

  let base_bindings = () => ({

    '@keydown.j.window'() {
      this.update_rowid(1);
    },
    '@keydown.shift.down.window'() {
      this.update_rowid(1);
    },
    '@keydown.shift.up.window'() {
      this.update_rowid(-1);
    },
    '@keydown.k.window'() {
      this.update_rowid(-1);
    },

    '@keydown.shift.right.window'() {
      this.update_colid(1);
    },
    '@keydown.l.window'() {
      this.update_colid(1);
    },
    '@keydown.shift.left.window'() {
      this.update_colid(-1);
    },
    '@keydown.h.window'() {
      this.update_colid(-1);
    },

    '@keydown.shift.g.window'() {
      this.update_rowid_to_max();
    },
    '@keydown.g.window'() {
      if (!this.$event.shiftKey) this.update_rowid_to_min()
    },

    '@keydown.!.window'() {
      this.toggle_key(this.colidx);
    },

    '@keydown.+.window'() {
      this.toggle_agg_col(this.colidx);
    },

    '@keydown.shift.f.window'() {
      const col_name = this.$refs[`col-${this.colidx}`].getAttribute("data-colname");
      this.performOp("f", {'cols': [col_name]});
    },

    '@keydown.f.window'() {
      if (this.$event.shiftKey) {
        return
      }
      const col_names = this.key_cols.map(colidx => this.$refs[`col-${colidx}`].getAttribute("data-colname"));
      this.performOp("f", {'cols': col_names});
    },

    '@keydown.shift.w.window'() {

      if (this.agg_col == undefined) {
        alert("pick a column to aggregate on")
        return
      }
      // this logic is repeating
      const key_col_names = this.key_cols.map(colidx => this.$refs[`col-${colidx}`].getAttribute("data-colname"));
      const pivot_col = this.$refs[`col-${this.colidx}`].getAttribute("data-colname");
      const agg_col = this.$refs[`col-${this.agg_col}`].getAttribute("data-colname");
      this.performOp("pivot", {'key_cols': key_col_names, 'pivot_col': pivot_col, 'agg_col': agg_col});
    },

    '@keydown.,.window'() {
      const value = cellToVal(this.$refs[`cell-${this.rowidx}-${this.colidx}`])
      const colidx = this.colidx

      // toggles the presence of (colidx, value) in filter_vals
      if (colidx in this.filter_vals) {
        const vals = this.filter_vals[colidx]
        if (vals.has(value)){
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

    '@keydown.".window'() {
      // filter
      const filters = [] // [(col, val), ...]
      for (const colidx in this.filter_vals) {
        this.filter_vals[colidx].forEach(value => filters.push([this.$refs[`col-${colidx}`].getAttribute("data-colname"), value]))
      }
      if (filters.length == 0) {
        alert("pick some values to filter on")
        return
      }
      this.performOp("fil", {'filters': filters, criterion: "any"});
    },

    "@keydown.window"() {
      const col_name = this.$refs[`col-${this.colidx}`].getAttribute("data-colname");
      if (this.$event.ctrlKey) {
        return
      }
      if (this.$event.key == '[') {
        this.performOp('sa', { 'params': col_name });
      } else if (this.$event.key == ']') {
        this.performOp('sd', { 'params': col_name });
      }
    }
  });

  Alpine.bind('base_sheet', () => ({
    ...base_bindings()
  }));

  function cellToVal(cell_el) {
    // check if cell is NULL or an empty string
    if (cell_el.classList.contains("null")) {
      return null
    } else {
      return cell_el.innerText;
    }
  }

  Alpine.bind('freq_sheet', (key_cols) => ({
    ...base_bindings(),

    '@keydown.enter.window'() {

      // key_cols is a list of column indices
      const filters = key_cols.map(j => [
        this.$refs[`col-${j}`].getAttribute("data-colname"),
        cellToVal(this.$refs[`cell-${this.rowidx}-${j}`])
      ])

      this.performOp("fac", { 'facets': filters });
    }
  }));

  function isFiltered(row_el, filter_vals) {
    // if `filter_vals` is empty return false
    if (Object.keys(filter_vals).length == 0) {
      return false
    }

    for (var j = 0, cell; cell = row_el.cells[j]; j++) {
      if (j in filter_vals && filter_vals[j].has(cellToVal(cell))) {
        return false
      }
    }
    return true
  }

  Alpine.bind('row', (idx) => ({
    ':class'() {
      return {
        'active': this.rowidx == idx,
        'filtered': isFiltered(this.$refs[`row-${idx}`], this.filter_vals),
      }
    }
  }));

  Alpine.bind('col', (j) => ({
    ':class'() {
      return {
        'active': (this.colidx == j),
        'key-col': this.key_cols.includes(j),
        'agg-col': this.agg_col == j
      }
    }

  }));

  Alpine.bind('cell', (i, j) => ({
    ':class'() {
      return {
        'filtered-val': (j in this.filter_vals) && (this.filter_vals[j].has(cellToVal(this.$refs[`cell-${i}-${j}`]))),
        'selected-cell': (this.rowidx == i) && (this.colidx == j),
        'selected-col': (this.colidx == j),
      }
    },

    'x-effect'() {
      if (this.rowidx == i && this.colidx == j) {
        this.$el.scrollIntoView({
          block: this.rowidx == 0 ? 'end' : 'nearest',
          inline: 'nearest'
        })
      }
    },

    '@mouseover'() {
      this.rowidx = i
      this.colidx = j
    },

  }));

});
