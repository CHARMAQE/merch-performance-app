package com.smollan.backend.entity;

import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.Table;

@Entity
@Table(name = "task_location_checkin")
public class TaskLocationChecin {
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

}
