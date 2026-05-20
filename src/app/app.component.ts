import { Component, inject } from '@angular/core';
import { RouterOutlet } from '@angular/router';
import { RxjsOperatorsService } from './services/rxjs-operators.service';
import { concatMap, delay, distinct, distinctUntilChanged, forkJoin, map, mergeMap, of, Subject, switchMap } from 'rxjs';
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
    //this.rxjsService.demoConcatMap();
    this.loadEmployeeById(1);
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

  /**
   * example of forkjoin
   */
  loadAllEmployeesAndOrders() {
    const orders$ = this.httpService.get<order[]>(
      `${this.baseURL}/api/orders`
    );
    const employees$ = this.httpService.get<employee[]>(
      `${this.baseURL}/api/employees`
    );

    forkJoin({ orders: orders$, employees: employees$ })
      .subscribe({
        next: (combinedData) => {
          console.log(combinedData.employees, combinedData.orders);
        },
        error: (err) => {
          console.error(err);
        }
      });
  }
  ngOnDestroy(): void { this.destroy$.next(); this.destroy$.complete(); }

}
