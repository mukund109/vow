document.addEventListener('alpine:init', () => {
  function performOp(op, params) {
    let data = { operation_type: op, params: params };
    sendPostRequest(data);
  }

  function performFilterOp(col, keyword) {
    let data = { operation_type: "fil", col: col, keyword: keyword }
    sendPostRequest(data);
  }

  function performFreqOp(cols) {
    // let data = {operation: {operation_type : "f", cols: cols}}
    let data = { operation_type: "f", cols: cols }
    sendPostRequest(data);
  }

  function sendPostRequest(data) {

    // strip out '/' from the url
    let path = window.location.pathname.replace('\/', '');

    fetch(`/sheets/${path}`, {
      method: "POST",
      body: JSON.stringify(data),
      headers: {
        'Content-Type': 'application/json'
      },
    }).then(res => {
      console.log(res);
      res.json().then(data => {
        console.log(data);
        if (data.new_sheet != undefined) {
          window.location.href = new URL(data.new_sheet, window.location.origin)
        }
      });
    });
  }

  Alpine.data('sheet', (num_rows, num_cols) => ({
    rowidx: 0,
    colidx: 0,
    key_cols: [],

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

    '@keydown.shift.f.window'() {
      col_name = this.$refs[`col-${this.colidx}`].innerText;
      performFreqOp([col_name]);
    },

    '@keydown.f.window'() {
      if (this.$event.shiftKey) {
        console.log("pressed shift-F");
        return
      }
      col_names = this.key_cols.map(colidx => this.$refs[`col-${colidx}`].innerText);
      performFreqOp(col_names);
    },

    "@keydown.window"() {
      col_name = this.$refs[`col-${this.colidx}`].innerText;
      if (this.$event.key == '[') {
        performOp('sa', col_name);
      } else if (this.$event.key == ']') {
        performOp('sd', col_name);
      }
    }
  });

  Alpine.bind('base_sheet', () => ({
    ...base_bindings()
  }));

  Alpine.bind('freq_sheet', (key_cols) => ({
    ...base_bindings(),

    '@keydown.enter.window'() {
      // col_name = this.$refs[`col-${this.colidx}`].innerText;
      col_name = key_cols[0]
      cell_el = this.$refs[`cell-${this.rowidx}-${this.colidx}`];
      // check if value is NULL or an empty string
      if (cell_el.classList.contains("null")) {
        cell_value = null
      } else {
        cell_value = cell_el.innerText;
      }
      performFilterOp(col_name, cell_value);
    }
  }));

  Alpine.bind('row', (idx) => ({
    ':class'() {
      return this.rowidx == idx ? 'active' : ''
    }

  }));

  Alpine.bind('col', (j) => ({
    ':class'() {
      return {
        'active': (this.colidx == j),
        'key-col': this.key_cols.includes(j)
      }
    }

  }));

  Alpine.bind('cell', (i, j) => ({
    ':class'() {
      return {
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
    }
  }));

});

