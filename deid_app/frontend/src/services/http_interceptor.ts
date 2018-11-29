import {HttpEvent, HttpHandler, HttpRequest} from '@angular/common/http';
import {HttpErrorResponse, HttpInterceptor} from '@angular/common/http';
import {Injectable} from '@angular/core';
import {Observable, throwError} from 'rxjs';
import {catchError} from 'rxjs/operators';

import {ErrorHandler} from './error_handler';

/**
 * Ensures that any error responses from the server are handled properly.
 */
@Injectable()
export class RequestInterceptor implements HttpInterceptor {
  constructor(private handler: ErrorHandler) {}

  /**
   * Implement the intercept function that gets called on every
   * incoming/outgoing call between app and server.
   */
  // tslint:disable:no-any error can be of any type by definition.
  intercept(request: HttpRequest<any>, next: HttpHandler):
      Observable<HttpEvent<any>> {
  // tslint:enable:no-any error can be of any type by definition.
    return next.handle(request).pipe(catchError((error, caught) => {
      this.handler.handleError(error);
      return throwError(error);
    }));
  }
}
