import {HTTP_INTERCEPTORS, HttpClientModule} from '@angular/common/http';
import {NgModule} from '@angular/core';
import {FormsModule, ReactiveFormsModule} from '@angular/forms';
import {BrowserModule} from '@angular/platform-browser';
import {BrowserAnimationsModule} from '@angular/platform-browser/animations';

import {BigQueryNewTable} from '../common/bigquery-new-table';
import {BigQueryTable} from '../common/bigquery-table';
import {SubmitComponent} from '../common/submit_component';
import {CompareDataComponent} from '../deidentify/compare-data/compare-data.component';
import {CsvUploadComponent} from '../deidentify/csv-upload/csv-upload.component';
import {CurrentJobsComponent} from '../deidentify/current-jobs/current-jobs.component';
import {DeidentifyComponent} from '../deidentify/deidentify.component';
import {RunDeidentifyComponent} from '../deidentify/run-deidentify/run-deidentify.component';
import {UploadNotesComponent} from '../deidentify/upload-notes/upload-notes.component';
import {DlpDemoComponent} from '../dlp-demo/dlp-demo.component';
import {DlpImageDemoComponent} from '../dlp-demo/dlp-image-demo/dlp-image-demo.component';
import {DlpTextDemoComponent} from '../dlp-demo/dlp-text-demo/dlp-text-demo.component';
import {EvalPipelineComponent} from '../evaluate/eval-pipeline/eval-pipeline.component';
import {EvalStatsComponent} from '../evaluate/eval-stats/eval-stats.component';
import {EvaluateComponent} from '../evaluate/evaluate.component';
import {DlpDemoService} from '../services/dlp-demo.service';
import {RequestInterceptor} from '../services/http_interceptor';
import {ErrorHandler} from '../services/error_handler';

import {AppComponent} from './app.component';
import {AppMaterialModule} from './material.module';
import {RoutingModule} from './routing.module';

@NgModule({
  declarations: [
    AppComponent,
    DeidentifyComponent,
    DlpDemoComponent,
    EvaluateComponent,
    UploadNotesComponent,
    RunDeidentifyComponent,
    BigQueryTable,
    BigQueryNewTable,
    CompareDataComponent,
    CurrentJobsComponent,
    DlpImageDemoComponent,
    DlpTextDemoComponent,
    EvalPipelineComponent,
    EvalStatsComponent,
    CsvUploadComponent,
    SubmitComponent,
  ],
  imports: [
    BrowserModule,
    BrowserAnimationsModule,
    AppMaterialModule,
    RoutingModule,
    HttpClientModule,
    FormsModule,
    ReactiveFormsModule,
  ],
  providers: [
    ErrorHandler,
    {provide: HTTP_INTERCEPTORS, useClass: RequestInterceptor, multi: true},
    DlpDemoService,
  ],
  bootstrap: [AppComponent]
})
export class AppModule {
}
