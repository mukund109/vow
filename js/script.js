document.addEventListener('alpine:init', () => {
  function performOp(op, params) {
    let data = {operation_type: op, params: params};
    sendPostRequest(data);
  }

  function performFreqOp(cols) {
    // let data = {operation: {operation_type : "f", cols: cols}}
    let data = {operation_type : "f", cols: cols}
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
          if (data.new_sheet != undefined){
            window.location.href = new URL(data.new_sheet, window.location.origin)
          }
        });
      });
  }

  Alpine.data('sheet', (num_rows, num_cols) => ({
    rowidx: 0,
    colidx: 0,

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

  '@keydown.shift.f.window'() {
    col_name = this.$refs[`col-${this.colidx}`].innerText;
    performFreqOp([col_name]);
  },

  "@keydown.window"() {
    col_name = this.$refs[`col-${this.colidx}`].innerText;
    if (this.$event.key == '['){
      performOp('sa', col_name);
    } else if (this.$event.key == ']'){
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
      cell_value = this.$refs[`cell-${this.rowidx}-${this.colidx}`].innerText;
      // will definitely break if there are colons in cell_value or col_name
      performOp('fil', `${col_name}:${cell_value}`);
    }
  }));

  Alpine.bind('row', (idx) => ({
    ':class'() {
      return this.rowidx == idx ? 'active' : ''
    }

  }));

  Alpine.bind('cell', (i, j) => ({
    ':class'() {
    if (this.rowidx == i && this.colidx == j) {
      return 'selected-cell'
    } else if (this.colidx == j) {
      return 'selected-col'
    } else {
      return {}
    }
  },

  'x-effect'() {
    if (this.rowidx == i && this.colidx == j) {
      this.$el.scrollIntoView({
        block: this.rowidx == 0 ? 'end': 'nearest',
        inline: 'nearest'
      })
    }
  }
}));

});

