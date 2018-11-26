import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { CompareDataComponent } from './compare-data.component';

describe('CompareDataComponent', () => {
  let component: CompareDataComponent;
  let fixture: ComponentFixture<CompareDataComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ CompareDataComponent ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(CompareDataComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
