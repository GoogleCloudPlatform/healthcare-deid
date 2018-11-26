import {NgModule} from '@angular/core';
import {
  MatToolbarModule,
  MatSidenavModule,
  MatIconModule,
  MatInputModule,
  MatListModule,
  MatButtonModule,
  MatFormFieldModule,
  MatTabsModule,
  MatButtonToggleModule,
  MatSelectModule,
  MatTableModule,
  MatProgressSpinnerModule,
  MatDividerModule,
  MatAutocompleteModule,
  MatSnackBarModule,
  MatCheckboxModule,
} from '@angular/material';

/**
 * The Angular Material Module. Groups material modules and components and
 * exports them to the AppModule.
 */
@NgModule({
  exports: [
    MatAutocompleteModule,
    MatListModule,
    MatIconModule,
    MatInputModule,
    MatToolbarModule,
    MatButtonModule,
    MatSidenavModule,
    MatTabsModule,
    MatFormFieldModule,
    MatButtonToggleModule,
    MatSelectModule,
    MatTableModule,
    MatProgressSpinnerModule,
    MatDividerModule,
    MatSnackBarModule,
    MatCheckboxModule,
  ]
})
export class AppMaterialModule {
}

