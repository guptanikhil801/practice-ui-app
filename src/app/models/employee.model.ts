import { order } from "./order.model";

export class employee {
    id!: number;
    name: string = "";
    email: string = "";
    createdAt?: Date = new Date();
    orders?: order[] = [];
}