import {Component, OnInit} from '@angular/core';
import {DlpDemoService} from '../services/dlp-demo.service';

@Component({
  selector: 'app-deidentify',
  templateUrl: './deidentify.component.html',
  styleUrls: ['./deidentify.component.css']
})
export class DeidentifyComponent implements OnInit {
  constructor(private dlpDemoService: DlpDemoService) {}

  ngOnInit() {
    this.dlpDemoService.refreshDeidJobs();
    this.dlpDemoService.refreshDatasets();
    this.dlpDemoService.refreshProject();
  }
}
