import {Component} from '@angular/core';
import {DlpDemoService} from '../../services/dlp-demo.service';

/**
 * This component accepts an image file and redacts it. The redacted version is
 * displayed once available
 */
@Component({
  selector: 'app-dlp-image-demo',
  templateUrl: './dlp-image-demo.component.html',
  styleUrls: ['./dlp-image-demo.component.css']
})
export class DlpImageDemoComponent {
  base64OrigImg?: string;
  base64RedactedImg?: string;
  mimeImageType?: string;

  constructor(private readonly dlpDemoService: DlpDemoService) {}

  /**
   * returns the header of a byte64 image in order to append it later to a valid
   * byte stream to display the image.
   */
  private getBase64Header(base64Img: string): string {
    return base64Img.split(',')[0];
  }

  /**
   * Processes a received image from the user and requests the redacted version
   * from the backend.
   * @param images the list of files that the user has selected. Only the first
   * file within the list will be processed.
   */
  redactImage(images: FileList): void {
    if (images.length < 1) {
      console.log('no image selected');
      return;
    }
    const reader = new FileReader();
    const selectedImage = images.item(0);
    this.mimeImageType = selectedImage.type;
    reader.onload = (doneEvent: Event) => {
      this.base64OrigImg = reader.result;
      this.dlpDemoService.redactImage(this.mimeImageType, this.base64OrigImg)
          .subscribe(data => {
            /* append the byte stream to the base64 header. */
            this.base64RedactedImg =
                `${this.getBase64Header(this.base64OrigImg)},${
                    data.redactedByteStream}`;
          });
    };
    reader.readAsDataURL(selectedImage);
  }
}
