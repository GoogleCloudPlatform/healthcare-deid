import {Component, OnDestroy, OnInit} from '@angular/core';
import {FormControl, FormGroup} from '@angular/forms';
import {MatTableDataSource} from '@angular/material/table';
import {combineLatest, Observable, Subscription} from 'rxjs';
import {filter, map} from 'rxjs/operators';
import {startWith, switchMap} from 'rxjs/operators';

import {DlpDemoService} from '../../services/dlp-demo.service';
import {EvalJob, EvalStats} from '../eval_job';

/**
 * This component displays the results generated in the statistics table after
 * running the Evaluation pipeline.
 */
@Component({
  selector: 'app-eval-stats',
  templateUrl: './eval-stats.component.html',
  styleUrls: ['./eval-stats.component.css', '../evaluate.component.css']
})
export class EvalStatsComponent implements OnInit, OnDestroy {
  private readonly subscriptions = new Subscription();

  jobsFilter: Observable<EvalJob[]>;
  statsJobForm = new FormControl('');
  displayTable = false;

  private statsResults: Observable<EvalStats[]>;
  dataSource = new MatTableDataSource();
  displayedColumns: string[] = [
    'infoType', 'recall', 'precision', 'fScore', 'truePositives',
    'falsePositives', 'falseNegatives'
  ];

  constructor(private dlpDemoService: DlpDemoService) {}

  ngOnInit() {
    /* Get an Observable of the user's input. */
    const jobUserInput =
        this.statsJobForm.valueChanges.pipe(startWith<string|EvalJob>(''));

    /* Combine the user's input and the evalJobs to generate filter value. */
    this.jobsFilter =
        combineLatest(this.dlpDemoService.evalJobs, jobUserInput)
            .pipe(map(([jobs, input]) => {
              let filterValue: string;
              if (typeof input === 'string') {
                filterValue = input.toLowerCase();
              } else {
                filterValue = input.name.toLowerCase();
              }
              return jobs.filter(
                  entry => entry.name.toLowerCase().includes(filterValue));
            }));

    /* Make an api call to retrieve new job stats */
    this.statsResults = jobUserInput.pipe(
        filter(input => typeof input !== 'string'),
        switchMap((job: EvalJob) => {
          this.displayTable = false;
          return this.dlpDemoService.getEvalStats(job.id);
        }));

    /* hookup the table to the stats results */
    this.subscriptions.add(this.statsResults.subscribe((stats: EvalStats[]) => {
      this.displayTable = true;
      this.dataSource.data = stats;
    }));
  }

  ngOnDestroy() {
    this.subscriptions.unsubscribe();
  }

  displayJob(job: EvalJob|undefined): string {
    if (job) {
      return job.name;
    }
    return '';
  }

  setFilterTable(filterValue: string): void {
    this.dataSource.filter = filterValue.trim().toLowerCase();
  }
}
