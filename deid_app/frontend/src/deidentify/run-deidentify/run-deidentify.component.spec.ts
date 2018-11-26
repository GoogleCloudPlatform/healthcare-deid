import {HttpClientTestingModule} from '@angular/common/http/testing';
import {async, ComponentFixture, TestBed} from '@angular/core/testing';
import {FormsModule, ReactiveFormsModule} from '@angular/forms';
import {BrowserAnimationsModule} from '@angular/platform-browser/animations';

import {AppMaterialModule} from '../../app/material.module';

import {RunDeidentifyComponent} from './run-deidentify.component';

describe('RunDeidentifyComponent', () => {
  let component: RunDeidentifyComponent;
  let fixture: ComponentFixture<RunDeidentifyComponent>;

  beforeEach(async(() => {
    TestBed
        .configureTestingModule({
          imports: [
            BrowserAnimationsModule, HttpClientTestingModule,
            ReactiveFormsModule, FormsModule, AppMaterialModule
          ],
          declarations: [RunDeidentifyComponent],
        })
        .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(RunDeidentifyComponent);
    component = fixture.componentInstance;
    component.ngOnInit();
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });

  it('should be invalid when empty', () => {
    expect(component.dlpForm.valid).toBeFalsy();
  });

  it('output table name should be valid', () => {
    component.outputTable.setValue('invalid table');
    expect(component.outputTable.valid).toBeFalsy();
    component.outputTable.setValue('33startWithNum');
    expect(component.outputTable.valid).toBeFalsy();
    component.outputTable.setValue('validName');
    expect(component.outputTable.valid).toBeTruthy();
  });
});
