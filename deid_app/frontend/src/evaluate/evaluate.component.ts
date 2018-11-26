import {Component, OnInit} from '@angular/core';
import {DlpDemoService} from '../services/dlp-demo.service';

@Component({
  selector: 'app-evaluate',
  templateUrl: './evaluate.component.html',
  styleUrls: ['./evaluate.component.css']
})
export class EvaluateComponent implements OnInit {
  constructor(private dlpDemoService: DlpDemoService) {}

  ngOnInit() {
    this.dlpDemoService.refreshEvalJobs();
    this.dlpDemoService.refreshDatasets();
    this.dlpDemoService.refreshProject();
  }
}
