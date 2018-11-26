import {HttpClientTestingModule} from '@angular/common/http/testing';
import {async, ComponentFixture, TestBed} from '@angular/core/testing';
import {BrowserAnimationsModule} from '@angular/platform-browser/animations';

import {AppMaterialModule} from '../../app/material.module';

import {CurrentJobsComponent} from './current-jobs.component';

describe('CurrentJobsComponent', () => {
  let component: CurrentJobsComponent;
  let fixture: ComponentFixture<CurrentJobsComponent>;

  beforeEach(async(() => {
    TestBed
        .configureTestingModule({
          imports: [
            BrowserAnimationsModule, HttpClientTestingModule,
            AppMaterialModule
          ],
          declarations: [CurrentJobsComponent],
        })
        .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(CurrentJobsComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
