import { NgModule } from '@angular/core';
import { RouterModule, Routes } from '@angular/router';

import { DeidentifyComponent } from '../deidentify/deidentify.component';
import { EvaluateComponent } from '../evaluate/evaluate.component';
import { DlpDemoComponent } from '../dlp-demo/dlp-demo.component';

const routes: Routes = [
  { path: '', redirectTo: '/dlpdemo', pathMatch: 'full' },
  { path: 'dlpdemo', component: DlpDemoComponent },
  { path: 'deidentify', component: DeidentifyComponent },
  { path: 'evaluate', component: EvaluateComponent },
];

@NgModule({
  imports: [
    RouterModule.forRoot(routes)
  ],
  exports: [ RouterModule ],
})
export class RoutingModule { }

