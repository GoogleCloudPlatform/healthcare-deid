import {NO_ERRORS_SCHEMA} from '@angular/core';
import {async, ComponentFixture, TestBed} from '@angular/core/testing';

import {EvaluateComponent} from './evaluate.component';

describe('EvaluateComponent', () => {
  let component: EvaluateComponent;
  let fixture: ComponentFixture<EvaluateComponent>;

  beforeEach(async(() => {
    TestBed
        .configureTestingModule(
            {declarations: [EvaluateComponent], schemas: [NO_ERRORS_SCHEMA]})
        .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(EvaluateComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
