import {NO_ERRORS_SCHEMA} from '@angular/core';
import {async, ComponentFixture, TestBed} from '@angular/core/testing';

import {DlpDemoComponent} from './dlp-demo.component';

describe('DlpDemoComponent', () => {
  let component: DlpDemoComponent;
  let fixture: ComponentFixture<DlpDemoComponent>;

  beforeEach(async(() => {
    TestBed
        .configureTestingModule(
            {declarations: [DlpDemoComponent], schemas: [NO_ERRORS_SCHEMA]})
        .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(DlpDemoComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
