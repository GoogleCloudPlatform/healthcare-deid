import {Injectable} from '@angular/core';
import {MatSnackBar} from '@angular/material/snack-bar';

/**
 * Represents an error returned from the DlpDemo backend.
 */
interface DlpServiceError {
  text: string;
  error: number;
}

/**
 * Handles errors and display their message in a snack bar.
 */
@Injectable()
export class ErrorHandler {
  constructor(
      public snackbar: MatSnackBar,
  ) {}

  /**
   * Opens a SnackBar with the error details for the user.
   */
  // tslint:disable-next-line:no-any error can be of any type by definition.
  handleError(err: any) {
    let message = 'Server Error';
    if (this.isDlpServiceError(err.error) && err.error.text) {
      message = err.error.text;
    }
    this.snackbar.open(message, 'close');
  }

  // tslint:disable-next-line:no-any error can be of any type by definition.
  private isDlpServiceError(errorObject: any): errorObject is DlpServiceError {
    return errorObject.text !== undefined && errorObject.error !== undefined &&
        typeof errorObject.text === 'string' &&
        typeof errorObject.error === 'number';
  }
}
