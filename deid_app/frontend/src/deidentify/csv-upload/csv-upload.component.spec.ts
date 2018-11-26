import {async, ComponentFixture, TestBed} from '@angular/core/testing';

import {CsvUploadComponent} from './csv-upload.component';

describe('CsvUploadComponent', () => {
  let component: CsvUploadComponent;
  let fixture: ComponentFixture<CsvUploadComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({declarations: [CsvUploadComponent]})
        .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(CsvUploadComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
