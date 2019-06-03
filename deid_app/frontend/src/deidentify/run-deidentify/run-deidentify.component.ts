import {Component, OnDestroy, OnInit, ViewChild} from '@angular/core';
import {FormBuilder, FormControl, FormGroup, Validators} from '@angular/forms';
import {MatSnackBar} from '@angular/material/snack-bar';
import {Observable, Subscription} from 'rxjs';
import {finalize} from 'rxjs/operators';

import {BigQueryNewTable} from '../../common/bigquery-new-table';
import {BigQueryTable} from '../../common/bigquery-table';
import {DisplayOption} from '../../common/display-option';
import {DlpDemoService} from '../../services/dlp-demo.service';
import {DlpPipelineRequest} from '../../services/dlp-demo.service';

/**
 * Represents different parameters that the dlp pipeline accepts.
 */
enum DlpParameters {
  INPUT_QUERY = 'input_query',
  INPUT_TABLE = 'input_table',
  DEID_TABLE = 'deid_table',
  FINDINGS_TABLE = 'findings_table',
  MAE_TABLE = 'mae_table',
  MAE_DIR = 'mae_dir',
}

/**
 * This component manages a form that enables the user to run the DLP pipeline.
 * The component ensures that the data entered is valid. Once the request ends,
 * the job details can be viewed in the current jobs component.
 */
@Component({
  selector: 'app-run-deidentify',
  templateUrl: './run-deidentify.component.html',
  styleUrls: [
    './run-deidentify.component.css',
    '../deidentify.component.css',
  ],
})
export class RunDeidentifyComponent implements OnInit, OnDestroy {
  readonly dlpParameters = DlpParameters;
  private readonly subscriptions = new Subscription();

  @ViewChild('submitComponent') submitComp;
  readonly submitPlaceholder = 'Submit';

  inputOptions: DisplayOption[] = [
    {value: this.dlpParameters.INPUT_TABLE, displayString: 'BiqQuery Table'},
    {value: this.dlpParameters.INPUT_QUERY, displayString: 'BigQuery Query'},
  ];

  outputOptions: DisplayOption[] = [
    {value: this.dlpParameters.DEID_TABLE, displayString: 'BigQuery'},
  ];

  findingsOptions: DisplayOption[] = [
    {value: this.dlpParameters.FINDINGS_TABLE, displayString: 'BigQuery'},
  ];

  maeOptions: DisplayOption[] = [
    {value: this.dlpParameters.MAE_TABLE, displayString: 'BigQuery'},
    {value: this.dlpParameters.MAE_DIR, displayString: 'Google Cloud Storage'},
  ];

  dlpForm = this.formBuilder.group({
    project: '',
    name: [new Date().toLocaleString(), Validators.required],
    input: this.formBuilder.group({
      method: ['', Validators.required],
      bqTable: BigQueryTable.buildEntry(),
      query: ['', Validators.required],
    }),
    output: this.formBuilder.group({
      method: ['', Validators.required],
      bqTable: BigQueryNewTable.buildEntry(),
    }),
    findings: this.formBuilder.group({
      method: ['', Validators.required],
      bqTable: BigQueryNewTable.buildEntry(),
    }),
    mae: this.formBuilder.group({
      method: ['', Validators.required],
      bqTable: BigQueryNewTable.buildEntry(),
      gcs: ['', Validators.required],
    }),
    batchSize: 1,
  });

  constructor(
      private dlpDemoService: DlpDemoService,
      public snackBar: MatSnackBar,
      private formBuilder: FormBuilder,
  ) {}

  get inputMethod(): FormControl {
    return this.dlpForm.get('input.method') as FormControl;
  }

  get bqTableInput(): FormGroup {
    return this.dlpForm.get('input.bqTable') as FormGroup;
  }

  get inputQuery(): FormControl {
    return this.dlpForm.get('input.query') as FormControl;
  }

  get outputMethod(): FormControl {
    return this.dlpForm.get('output.method') as FormControl;
  }

  get bqTableOutput(): FormGroup {
    return this.dlpForm.get('output.bqTable') as FormGroup;
  }

  get findingsMethod(): FormControl {
    return this.dlpForm.get('findings.method') as FormControl;
  }

  get bqTableFindings(): FormGroup {
    return this.dlpForm.get('findings.bqTable') as FormGroup;
  }

  get maeMethod(): FormControl {
    return this.dlpForm.get('mae.method') as FormControl;
  }

  get bqTableMae(): FormGroup {
    return this.dlpForm.get('mae.bqTable') as FormGroup;
  }

  ngOnInit() {
    /* Get the project name from the server. */
    this.subscriptions.add(this.dlpDemoService.project.subscribe(
        project => this.dlpForm.patchValue({project})));

    /* Change dlpForm validation needs based on the selected user input. */
    this.subscriptions.add(this.inputMethod.valueChanges.subscribe(method => {
      if (method === this.dlpParameters.INPUT_QUERY) {
        this.bqTableInput.disable();
        this.inputQuery.enable();
      } else if (method === this.dlpParameters.INPUT_TABLE) {
        this.bqTableInput.enable();
        this.inputQuery.disable();
      }
    }));

    this.subscriptions.add(
        this.maeMethod.valueChanges.subscribe(method => {
          if (method === this.dlpParameters.MAE_TABLE) {
            this.bqTableMae.enable();
            this.dlpForm.get('mae.gcs').disable();
          } else if (method === this.dlpParameters.MAE_DIR) {
            this.bqTableMae.disable();
            this.dlpForm.get('mae.gcs').enable();
          }
        }));
  }

  ngOnDestroy() {
    this.subscriptions.unsubscribe();
  }

  private getTableName(bqTable: FormGroup): string {
    const dataset = bqTable.get('dataset').value;
    const table = bqTable.get('table').value;
    return `${dataset}.${table}`;
  }

  /**
   * If the form is valid, the onSubmit is going to turn the form into a DeidJob
   * and send a request with that job to the server. A spinner will exist as
   * long as the call is running.
   */
  private onSubmit(): void {
    const job: DlpPipelineRequest = {
      name: '',
      inputMethod: '',
      inputInfo: '',
      outputMethod: '',
      outputInfo: '',
    };
    const formVal = this.dlpForm.value;
    job.name = formVal.name;
    job.inputMethod = formVal.input.method;
    if (job.inputMethod === this.dlpParameters.INPUT_TABLE) {
      job.inputInfo = this.getTableName(this.bqTableInput);
    } else if (job.inputMethod === this.dlpParameters.INPUT_QUERY) {
      job.inputInfo = formVal.input.query;
    }

    job.outputMethod = formVal.output.method;
    if (job.outputMethod === this.dlpParameters.DEID_TABLE) {
      job.outputInfo = this.getTableName(this.bqTableOutput);
    }

    job.findingsTable = this.getTableName(this.bqTableFindings);
    if (formVal.mae.method === this.dlpParameters.MAE_TABLE) {
      job.maeTable = this.getTableName(this.bqTableMae);
    } else if (formVal.mae.method === this.dlpParameters.MAE_DIR) {
      job.maeDir = `gs://${formVal.mae.gcs}`;
    }
    job.batchSize = this.dlpForm.get('batchSize').value;

    this.dlpForm.patchValue({
      name: new Date().toLocaleString(),
    });
    this.submitComp.waiting = true;
    this.dlpDemoService.deidentifyData(job)
        .pipe(finalize(() => this.submitComp.waiting = false))
        .subscribe(res => {
          let message: string;
          if (res.result === 'success') {
            message = 'Request Succeeded!';
          } else {
            message = 'Request Failed';
          }
          this.snackBar.open(message, 'Dismiss', {
            duration: 3000,
          });
          this.dlpDemoService.refreshDeidJobs();
        });
  }
}
