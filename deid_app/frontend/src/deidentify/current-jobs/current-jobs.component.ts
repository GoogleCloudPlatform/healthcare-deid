import {Component, OnDestroy, OnInit} from '@angular/core';
import {MatTableDataSource} from '@angular/material';
import {Subscription} from 'rxjs';

import {DeidJob} from '../../deid-job';
import {DlpDemoService} from '../../services/dlp-demo.service';

/**
 * Displays the deidentify jobs that the user have created and ran.
 */
@Component({
  selector: 'app-current-jobs',
  templateUrl: './current-jobs.component.html',
  styleUrls: [
    './current-jobs.component.css',
    '../deidentify.component.css',
  ]
})
export class CurrentJobsComponent implements OnInit, OnDestroy {
  private readonly subscriptions = new Subscription();

  dataSource = new MatTableDataSource();
  displayedColumns: string[] =
      ['id', 'name', 'originalQuery', 'deidTable', 'status', 'timestamp'];

  constructor(private dlpDemoService: DlpDemoService) {}

  ngOnInit() {
    /* Updates the dataSource whenever the with Deid Jobs. */
    this.subscriptions.add(
        this.dlpDemoService.deidJobs.subscribe((jobs: DeidJob[]) => {
          this.dataSource.data = jobs;
        }));
  }

  ngOnDestroy() {
    this.subscriptions.unsubscribe();
  }
}
