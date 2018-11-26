import {Component, Input, OnInit} from '@angular/core';
import {FormGroup} from '@angular/forms';

/**
 * Submit with spinner while running.
 */
@Component({
  selector: 'app-submit',
  templateUrl: './submit_component.html',
  styleUrls: [
    './submit_component.css',
  ]
})
export class SubmitComponent {
  @Input() submitPlaceholder: string;
  @Input() submitForm: FormGroup;
  waiting = false;
}
