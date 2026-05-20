import { Component, inject } from '@angular/core';
import { RouterOutlet } from '@angular/router';
import { RxjsOperatorsService } from './services/rxjs-operators.service';
import { forkJoin, map, Subject } from 'rxjs';
import { UserComponent } from "./user/user.component";
import { HttpClient } from '@angular/common/http';
import { order } from './models/order.model';
import { employee } from './models/employee.model';
import { error } from 'node:console';

@Component({
  selector: 'app-root',
  imports: [RouterOutlet, UserComponent],
  templateUrl: './app.component.html',
  styleUrl: './app.component.scss'
})
export class AppComponent {
  title = 'practice-ui-app';
  private rxjsService = inject(RxjsOperatorsService); 
  private http = inject(HttpClient); 
  private destroy$ = new Subject<void>();
  private baseURL = 'http://localhost:5292';

  ngOnInit(): void {
    // this.loadAllEmployeesAndOrders();
    this.rxjsService.demoConcatMap();
    //this.rxjsService.demoConcatMap();
    //this.loadEmployeeById(1);
  }

  loadEmployeeById(id: number) {
    const employee$ = this.http.get<employee>(`${this.baseURL}/api/employees/${id}`);
    employee$.subscribe({
        next: (empdata: employee) => {
          console.log(empdata);
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
    const orders$ = this.http.get<order[]>(
      `${this.baseURL}/api/orders`
    );

    const employees$ = this.http.get<employee[]>(
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
