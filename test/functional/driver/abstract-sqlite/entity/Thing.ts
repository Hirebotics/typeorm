import { Column, Entity, PrimaryGeneratedColumn } from "../../../../../src"

@Entity()
export class Thing {
    @PrimaryGeneratedColumn()
    id: number

    @Column()
    name: string
}
