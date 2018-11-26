import {async, ComponentFixture, TestBed} from '@angular/core/testing';

import {EvalStatsComponent} from './eval-stats.component';

describe('EvalStatsComponent', () => {
  let component: EvalStatsComponent;
  let fixture: ComponentFixture<EvalStatsComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({declarations: [EvalStatsComponent]})
        .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(EvalStatsComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
