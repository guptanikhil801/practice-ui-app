import { bootstrapApplication } from '@angular/platform-browser';
import { appConfig } from './app/app.config';
import { AppComponent } from './app/app.component';
import { provideHttpClient } from '@angular/common/http';
import { UserComponent } from './app/user/user.component';

bootstrapApplication(AppComponent, {
  providers: [
    provideHttpClient(),
    //UserComponent
  ]
}).catch(err => console.error(err));
