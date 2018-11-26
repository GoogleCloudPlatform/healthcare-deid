import {async, ComponentFixture, TestBed} from '@angular/core/testing';

import {EvalPipelineComponent} from './eval-pipeline.component';

describe('EvalPipelineComponent', () => {
  let component: EvalPipelineComponent;
  let fixture: ComponentFixture<EvalPipelineComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({declarations: [EvalPipelineComponent]})
        .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(EvalPipelineComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
