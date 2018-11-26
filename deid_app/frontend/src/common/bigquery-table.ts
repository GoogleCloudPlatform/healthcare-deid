import {Component, Input, OnInit} from '@angular/core';
import {FormBuilder, FormControl, FormGroup, Validators} from '@angular/forms';
import {Observable} from 'rxjs';
import {filter, map, switchMap} from 'rxjs/operators';

import {DlpDemoService} from '../services/dlp-demo.service';

/**
 * A selector for an existing table from the user's BQ tables.
 */
@Component({
  selector: 'bigquery-table',
  templateUrl: './bigquery-table.html',
  styleUrls: [
    './bigquery-table.css',
  ]
})
export class BigQueryTable implements OnInit {
  @Input() bqTableForm: FormGroup;

  bqTables: Observable<string[]>;

  get dataset(): FormControl {
    return this.bqTableForm.get('dataset') as FormControl;
  }

  get table(): FormControl {
    return this.bqTableForm.get('table') as FormControl;
  }

  constructor(
      protected dlpDemoService: DlpDemoService,
      protected formBuilder: FormBuilder,
  ) {}

  ngOnInit() {
    /* Configure a listener to get the tables of an input dataset. */
    this.bqTables = this.dataset.valueChanges.pipe(
        filter(dataset => dataset !== null && dataset !== ''),
        switchMap(dataset => this.dlpDemoService.getTables(dataset)),
        map(datasetTables => datasetTables.tables));
  }

  /**
   * Creates a FormGroup object to be associated with the BigQueryTable
   * component.
   */
  static buildEntry(): FormGroup {
    return new FormGroup({
      dataset: new FormControl('', Validators.required),
      table: new FormControl('', Validators.required),
    });
  }
}
