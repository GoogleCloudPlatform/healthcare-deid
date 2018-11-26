import {NO_ERRORS_SCHEMA} from '@angular/core';
import {async, ComponentFixture, TestBed} from '@angular/core/testing';

import {DeidentifyComponent} from './deidentify.component';

describe('DeidentifyComponent', () => {
  let component: DeidentifyComponent;
  let fixture: ComponentFixture<DeidentifyComponent>;

  beforeEach(async(() => {
    TestBed
        .configureTestingModule(
            {declarations: [DeidentifyComponent], schemas: [NO_ERRORS_SCHEMA]})
        .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(DeidentifyComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
