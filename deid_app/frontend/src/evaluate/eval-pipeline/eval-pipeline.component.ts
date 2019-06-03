import {Component, OnDestroy, OnInit} from '@angular/core';
import {FormBuilder, FormControl, FormGroup, Validators} from '@angular/forms';
import {MatSnackBar} from '@angular/material/snack-bar';
import {Observable, Subscription} from 'rxjs';
import {filter, finalize, map, startWith, switchMap} from 'rxjs/operators';

import {BigQueryNewTable} from '../../common/bigquery-new-table';
import {BigQueryTable} from '../../common/bigquery-table';
import {DisplayOption} from '../../common/display-option';
import {DlpDemoService} from '../../services/dlp-demo.service';
import {EvalBqInput, EvalBqOutput} from '../../services/dlp-demo.service';
import {EvalGcsInput, EvalGcsOutput} from '../../services/dlp-demo.service';
import {EvalPipelineRequest} from '../../services/dlp-demo.service';

/**
 * Represents different supported Evaluation stores.
 */
enum EvalStores {
  BIGQUERY = 'bigquery',
  GCS = 'gcs',
}

/**
 * Represents a bigquery table reference.
 */
interface BqTable {
  dataset: string;
  table: string;
}

/**
 * This component manages a form that enables the user to run the evaluation
 * pipeline.
 */
@Component({
  selector: 'app-eval-pipeline',
  templateUrl: './eval-pipeline.component.html',
  styleUrls: [
    './eval-pipeline.component.css',
    '../evaluate.component.css',
  ]
})
export class EvalPipelineComponent implements OnInit, OnDestroy {
  readonly evalStores = EvalStores;
  private readonly subscriptions = new Subscription();

  evalForm = this.formBuilder.group({
    project: '',
    name: [new Date().toLocaleString(), Validators.required],
    input: this.formBuilder.group({
      method: ['', Validators.required],
      bigquery: this.formBuilder.group({
        query: ['', Validators.required],
        golden: BigQueryTable.buildEntry(),
      }),
      gcs: this.formBuilder.group({
        pattern: ['', Validators.required],
        golden: ['', Validators.required],
      }),
    }),
    output: this.formBuilder.group({
      method: ['', Validators.required],
      bigquery: this.formBuilder.group({
        stats: BigQueryNewTable.buildEntry(),
        debug: BigQueryNewTable.buildEntry(),
      }),
      gcs: this.formBuilder.group({
        dir: ['', Validators.required],
        debug: [false, Validators.required],
      }),
    }),
  });

  pipelineRunning = false;

  /* Input Options */
  inputOptions: DisplayOption[] = [
    {value: this.evalStores.BIGQUERY, displayString: 'BiqQuery'},
    {value: this.evalStores.GCS, displayString: 'Google Cloud Storage'},
  ];

  /* Output Options */
  outputOptions: DisplayOption[] = [
    {value: this.evalStores.BIGQUERY, displayString: 'BiqQuery'},
    {value: this.evalStores.GCS, displayString: 'Google Cloud Storage'},
  ];

  constructor(
      private dlpDemoService: DlpDemoService,
      private formBuilder: FormBuilder,
      public snackBar: MatSnackBar,
  ) {}

  get inputMethod(): FormControl {
    return this.evalForm.get('input.method') as FormControl;
  }

  get goldenBqTable(): FormGroup {
    return this.evalForm.get('input.bigquery.golden') as FormGroup;
  }

  get outputMethod(): FormControl {
    return this.evalForm.get('output.method') as FormControl;
  }

  get statsBqTable(): FormGroup {
    return this.evalForm.get('output.bigquery.stats') as FormGroup;
  }

  get debugBqTable(): FormGroup {
    return this.evalForm.get('output.bigquery.debug') as FormGroup;
  }

  ngOnInit() {
    /* Get the project name from the server. */
    this.subscriptions.add(this.dlpDemoService.project.subscribe(
        project => this.evalForm.patchValue({project})));

    /**
     * Change evalForm input validation needs based on the selected input
     * method.
     */
    this.subscriptions.add(this.inputMethod.valueChanges.subscribe(method => {
      if (method === this.evalStores.GCS) {
        this.evalForm.get('input.gcs').enable();
        this.evalForm.get('input.bigquery').disable();
      } else if (method === this.evalStores.BIGQUERY) {
        this.evalForm.get('input.gcs').disable();
        this.evalForm.get('input.bigquery').enable();
      }
    }));

    /**
     * Change evalForm output validation needs based on the selected output
     * method.
     */
    this.subscriptions.add(this.outputMethod.valueChanges.subscribe(method => {
      if (method === this.evalStores.GCS) {
        this.evalForm.get('output.gcs').enable();
        this.evalForm.get('output.bigquery').disable();
      } else if (method === this.evalStores.BIGQUERY) {
        this.evalForm.get('output.gcs').disable();
        this.evalForm.get('output.bigquery').enable();
      }
    }));
  }

  ngOnDestroy() {
    this.subscriptions.unsubscribe();
  }

  private getTableName({dataset, table}: BqTable): string {
    return `${dataset}.${table}`;
  }

  onSubmit(): void {
    let input: EvalGcsInput|EvalBqInput;
    const evalFormInput = this.evalForm.value.input;
    if (evalFormInput.method === this.evalStores.GCS) {
      input = {
        gcs: {
          pattern: `gs://${evalFormInput.gcs.pattern}`,
          golden: `gs://${evalFormInput.gcs.golden}`,
        }
      };
    } else if (evalFormInput.method === this.evalStores.BIGQUERY) {
      input = {
        bigquery: {
          query: evalFormInput.bigquery.query,
          golden: this.getTableName(evalFormInput.bigquery.golden),
        }
      };
    }

    let output: EvalGcsOutput|EvalBqOutput;
    const evalFormOutput = this.evalForm.value.output;
    if (this.outputMethod.value === this.evalStores.GCS) {
      output = {
        gcs: {
          dir: `gs://${evalFormOutput.gcs.dir}`,
          debug: evalFormOutput.gcs.debugGcs,
        }
      };
    } else if (this.outputMethod.value === this.evalStores.BIGQUERY) {
      output = {
        bigquery: {
          stats: this.getTableName(evalFormOutput.bigquery.stats),
          debug: this.getTableName(evalFormOutput.bigquery.debug),
        }
      };
    }

    const request: EvalPipelineRequest = {
      name: this.evalForm.get('name').value,
      output,
      input,
    };

    this.pipelineRunning = true;
    this.subscriptions.add(this.dlpDemoService.evaluateFindings(request)
                               .pipe(finalize(() => {
                                 this.pipelineRunning = false;
                                 this.evalForm.patchValue({
                                   name: new Date().toLocaleString(),
                                 });
                               }))
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
                               }));
  }
}
