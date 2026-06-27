import { Component, inject } from '@angular/core';
import { RouterOutlet } from '@angular/router';
import { RxjsOperatorsService } from './services/rxjs-operators.service';
import { concatMap, debounceTime, delay, distinct, distinctUntilChanged, forkJoin, map, mergeMap, of, Subject, switchMap } from 'rxjs';
import { UserComponent } from "./user/user.component";
import { HttpClient } from '@angular/common/http';
import { order } from './models/order.model';
import { employee } from './models/employee.model';
import { error } from 'node:console';

@Component({
  selector: 'app-root',
  imports: [UserComponent],
  templateUrl: './app.component.html',
  styleUrl: './app.component.scss'
})
export class AppComponent {
  title = 'practice-ui-app';
  private rxjsService = inject(RxjsOperatorsService); 
  private httpService = inject(HttpClient); 
  private destroy$ = new Subject<void>();
  private baseURL = 'http://localhost:5292';

  ngOnInit(): void {
   this.switchMapExample();
  }

  switchMapExample() {
    of(1, 2, 3, 4, 5).pipe(
      debounceTime(300),
      switchMap((data) => of(data))
    ).subscribe(
      {
        next: (datai) => { console.log(datai); },
        error: (err) => { console.error(err); }
      }
    )
  }

  loadEmployeeById(id: number) {
    const employee$ = this.httpService.get<employee>(`${this.baseURL}/api/employees/${id}`);
    employee$.subscribe({
      next: (employeeData) => {
        console.log(employeeData);
      },
      error: (err) => {
        console.error(err);
      }
    });
  }

  loadAllEmployeesAndOrders() {

    const orders$ = this.httpService.get<order[]>(`${this.baseURL}/api/orders/1002`);
    const employees$ = this.httpService.get<employee[]>(`${this.baseURL}/api/employees/1002/employee-details-with-orders`);
    forkJoin({orders: orders$, employees: employees$}).subscribe(
      {
        next: (data) => {console.log(data.employees, data.orders);},
        error: (err) => {console.error(err);}
      }
    );
  }

  ngOnDestroy(): void { this.destroy$.next(); this.destroy$.complete(); }

}



