import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { UploadNotesComponent } from './upload-notes.component';

describe('UploadNotesComponent', () => {
  let component: UploadNotesComponent;
  let fixture: ComponentFixture<UploadNotesComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ UploadNotesComponent ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(UploadNotesComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
