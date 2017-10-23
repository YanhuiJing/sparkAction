package com.spark.base.model;

import lombok.*;

import java.io.Serializable;

@Getter
@Setter
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class Student implements Serializable{
    private int id;
    private String name;
    private int age;

}
