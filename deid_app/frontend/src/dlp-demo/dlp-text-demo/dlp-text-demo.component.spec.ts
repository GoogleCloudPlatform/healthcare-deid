import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { DlpTextDemoComponent } from './dlp-text-demo.component';

describe('DlpTextDemoComponent', () => {
  let component: DlpTextDemoComponent;
  let fixture: ComponentFixture<DlpTextDemoComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ DlpTextDemoComponent ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(DlpTextDemoComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
