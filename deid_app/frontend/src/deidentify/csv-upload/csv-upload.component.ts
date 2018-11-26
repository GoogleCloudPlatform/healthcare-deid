import {Component, OnDestroy, OnInit, ViewChild} from '@angular/core';
import {FormControl, FormGroup, Validators} from '@angular/forms';
import {MatSnackBar} from '@angular/material';
import {Observable, Subscription} from 'rxjs';
import {finalize} from 'rxjs/operators';

import {BigQueryNewTable} from '../../common/bigquery-new-table';
import {DlpDemoService} from '../../services/dlp-demo.service';

/**
 * Allows the user to upload a csv file to BigQuery table.
 */
@Component({
  selector: 'app-csv-upload',
  templateUrl: './csv-upload.component.html',
  styleUrls: [
    './csv-upload.component.css',
    '../deidentify.component.css',
  ]
})
export class CsvUploadComponent implements OnInit, OnDestroy {
  private readonly subscriptions = new Subscription();

  @ViewChild('submitComponent') submitComp;
  readonly submitPlaceholder = 'Upload';

  csvForm = new FormGroup({
    project: new FormControl(''),
    bqTable: BigQueryNewTable.buildEntry(),
    file: new FormControl('', Validators.required),
  });

  constructor(
      private dlpDemoService: DlpDemoService,
      public snackBar: MatSnackBar,
  ) {}

  get selectedCsv(): FormControl {
    return this.csvForm.get('file') as FormControl;
  }

  get bqTable(): FormGroup {
    return this.csvForm.get('bqTable') as FormGroup;
  }

  ngOnInit() {
    /* Get the project name from the server. */
    this.subscriptions.add(this.dlpDemoService.project.subscribe(
        project => this.csvForm.patchValue({project})));
  }

  ngOnDestroy() {
    this.subscriptions.unsubscribe();
  }

  /**
   * Reads and uploads a CSV from the user to the backend. The file is stored
   * in BigQuery.
   * @param csvFiles the list of files that the user has selected. Only the
   * first file within the list will be processed.
   */
  selectCsv(csvFiles: FileList): void {
    if (csvFiles.length < 1) {
      return;
    }
    const selectedCsv = csvFiles.item(0);
    this.selectedCsv.setValue(selectedCsv);
  }

  submit() {
    const formData = new FormData();
    const formVal = this.csvForm.value;
    formData.append('dataset', formVal.bqTable.dataset);
    formData.append('table', formVal.bqTable.table);
    formData.append('csv', formVal.file, formVal.file.name);
    this.submitComp.waiting = true;
    this.dlpDemoService.uploadCsv(formData)
        .pipe(finalize(() => this.submitComp.waiting = false))
        .subscribe(res => {
          const message = 'Upload successful!';
          this.snackBar.open(message, 'Dismiss', {
            duration: 3000,
          });
        });
  }
}
