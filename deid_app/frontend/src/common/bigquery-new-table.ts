import {Component} from '@angular/core';
import {FormBuilder, FormControl, FormGroup, Validators} from '@angular/forms';
import {combineLatest, Observable} from 'rxjs';
import {map, startWith} from 'rxjs/operators';

import {DlpDemoService} from '../services/dlp-demo.service';

import {BigQueryTable} from './bigquery-table';


/**
 * A selector for a new table from the user's BQ tables.
 */
@Component({
  selector: 'bigquery-new-table',
  templateUrl: './bigquery-new-table.html',
  styleUrls: [
    './bigquery-new-table.css',
  ]
})
export class BigQueryNewTable extends BigQueryTable {
  private tableFilter: Observable<string[]>;

  constructor(
      dlpDemoService: DlpDemoService,
      formBuilder: FormBuilder,
  ) {
    super(dlpDemoService, formBuilder);
  }

  ngOnInit() {
    super.ngOnInit();

    /* Get an Observable of the user's input. */
    const userInput = this.table.valueChanges.pipe(startWith(''));

    /* Combine the user's input and the bqTables to generate filter value. */
    this.tableFilter =
        combineLatest(userInput, this.bqTables)
            .pipe(map(([input, tables]) => this.includesFilter(input, tables)));
  }

  /**
   * Filters a list based on whether an entry contains a value as a subsequence.
   * @param value tests the list entries against.
   * @param list the original list that the filter will go through.
   */
  private includesFilter(value: string, list: string[]): string[] {
    if (!list) {
      return [];
    }
    const filterValue = value.toLowerCase();
    return list.filter(entry => entry.toLowerCase().includes(filterValue));
  }

  static buildEntry(): FormGroup {
    return new FormGroup({
      dataset: new FormControl('', Validators.required),
      table: new FormControl(
          '',
          [Validators.required, Validators.pattern('^[A-Za-z][A-Za-z0-9_]*$')]),
    });
  }
}
