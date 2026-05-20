import { Component, OnInit, inject } from '@angular/core';
import { CommonModule } from '@angular/common';
import { FormsModule } from '@angular/forms';
import { UserService } from '../services/user.service';
import { User } from '../models/user';
import { filter, map } from 'rxjs';

@Component({
  selector: 'app-user',
  standalone: true,
  imports: [CommonModule, FormsModule],
  templateUrl: './user.component.html'
})
export class UserComponent implements OnInit {

  private userService = inject(UserService);

  users: User[] = [];

  user: User = {
    name: '',
    email: ''
  };

  ngOnInit(): void {
   // this.getUserByEmail('');

  }

  getUserByEmail(email: string) {
    this.userService.getUsers()
      .pipe(
        map((users)=>
          users.filter(user => user.email === email)
        )
      )
      .subscribe(
         (data: User[]) => {
          this.users = data;
        }
      );
  }
  

  // GET
  getUsers() {
    this.userService.getUsers().pipe().subscribe({
      next: (data) => {
        this.users = data;
      },
      error: (err) => {
        console.error(err);
      }
    });
  }


  // POST
  addUser() {

    this.userService.createUser(this.user)
      .subscribe({
        next: (response) => {
          this.users.push(response);
          this.user = {
            name: '',
            email: ''
          };
        },
        error: (err) => {
          console.error(err);
        }
      });
  }

  // PUT
  updateUser(user: User) {

    const updatedUser = {
      ...user,
      name: user.name + ' Updated'
    };

    this.userService.updateUser(user.id!, updatedUser)
      .subscribe({
        next: (response) => {
          console.log('Updated', response);
        },
        error: (err) => {
          console.error(err);
        }
      });
  }

  // DELETE
  deleteUser(id: number) {

    this.userService.deleteUser(id)
      .subscribe({
        next: () => {
          this.users = this.users.filter(x => x.id !== id);
        },
        error: (err) => {
          console.error(err);
        }
      });
  }
}