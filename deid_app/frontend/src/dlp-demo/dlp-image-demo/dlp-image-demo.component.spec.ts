import {HttpClientTestingModule} from '@angular/common/http/testing';
import {async, ComponentFixture, TestBed} from '@angular/core/testing';
import {By} from '@angular/platform-browser';

import {DlpImageDemoComponent} from './dlp-image-demo.component';

describe('DlpImageDemoComponent', () => {
  let component: DlpImageDemoComponent;
  let fixture: ComponentFixture<DlpImageDemoComponent>;

  beforeEach(async(() => {
    TestBed
        .configureTestingModule({
          imports: [HttpClientTestingModule],
          declarations: [DlpImageDemoComponent]
        })
        .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(DlpImageDemoComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });

  it('should handle file change event', () => {
    const input =
        fixture.debugElement.query(By.css('input[type=file]')).nativeElement;

    spyOn(component, 'redactImage');
    input.dispatchEvent(new Event('change'));
    expect(component.redactImage).toHaveBeenCalled();
  });
});
