import {Component, OnInit} from '@angular/core';
import {FormControl, FormGroup} from '@angular/forms';
import {combineLatest, Observable} from 'rxjs';
import {map, startWith, switchMap} from 'rxjs/operators';

import {DeidJob} from '../deid_job';
import {DlpDemoService, NoteMetaData} from '../../services/dlp-demo.service';
import {Annotation, TextVisualizerData} from '../../services/dlp-demo.service';

/**
 * Displays highlighted text outlining the original and redacted versions of a
 * selected note.
 */
@Component({
  selector: 'app-compare-data',
  templateUrl: './compare-data.component.html',
  styleUrls: ['./compare-data.component.css', '../deidentify.component.css']
})
export class CompareDataComponent implements OnInit {
  noteToCompare: Observable<TextVisualizerData[]>;

  compareForm = new FormGroup({
    job: new FormControl(''),
    record: new FormControl(''),
  });

  jobsFilter: Observable<DeidJob[]>;

  records: Observable<NoteMetaData[]>;

  readonly FONTSIZE = '16px';

  constructor(private dlpDemoService: DlpDemoService) {}

  get job(): FormControl {
    return this.compareForm.get('job') as FormControl;
  }

  get record(): FormControl {
    return this.compareForm.get('record') as FormControl;
  }

  ngOnInit() {
    /* The list of Deid jobs that were run through this web app. */
    const jobsList = this.dlpDemoService.deidJobs;

    /**
     * This can be either the user's input or the actual record that a user
     * selects from the dropdown menu.
     */
    const jobUserInput =
        this.job.valueChanges.pipe(startWith<string|DeidJob>(''));

    /**
     * Initiates a call to get the different records that exist within a note
     * that was selected from a job.
     */
    this.jobsFilter =
        combineLatest(jobUserInput, jobsList).pipe(map(([input, jobs]) => {
          let filterValue: string;
          if (typeof input === 'string') {
            filterValue = input.toLowerCase();
          } else {
            this.records = this.dlpDemoService.getNotesMetadata(input.id);
            filterValue = input.name.toLowerCase();
          }
          return jobs.filter(
              entry => entry.name.toLowerCase().includes(filterValue));
        }));

    /**
     * Initiates a call to get the note to display a highlighted original and
     * deidentified version of.
     */
    this.noteToCompare = this.record.valueChanges.pipe(switchMap(
        (record) => this.dlpDemoService.getNoteHighlights(
            this.job.value.id, record.recordNumber)));
  }

  /**
   * Gets the job name, in case the job existed.
   */
  displayJob(job: DeidJob): string {
    if (job) {
      return job.name;
    }
    return '';
  }

  /**
   * Gets the original range of the note that is not deidentified.
   */
  getOrigText(entry: TextVisualizerData): string {
    return entry.quote;
  }

  /**
   * Gets the deidentified range of the note in case the range should be
   * highlighted.
   */
  getDeidText(entry: TextVisualizerData): string {
    if (entry.annotation !== Annotation.HIGHLIGHTED) {
      return entry.quote;
    }
    return entry.replacement;
  }

  /**
   * Returns the color to use to highlight a specific info type for the range
   * being targeted.
   */
  getBackgroundColor(entry: TextVisualizerData): string {
    if (entry.annotation === Annotation.HIGHLIGHTED) {
      return entry.color;
    }
    return 'none';
  }
}
