with user as
(
    Select Employee_id ,
           Employee_name ,
           Dept_Id ,
           Email ,
           Phone ,
           Address ,
           Hire_date ,
           Employee_Status
           from {{source('src','employee')}}
           --DBT_DEV.SRC.Employee
           
)
--Main Select
Select cast(employee_id as integer) as employee_id,
       employee_name,
       cast(dept_id as integer) as department_id,
       email,
       cast(phone as integer) as phone,
        Address ,
        Hire_date ,
        Employee_Status ,
        md5(nvl(cast(employee_name as varchar()),'')||nvl(cast(dept_id as varchar()),'')||nvl(cast(email as varchar()),'')) as md5_column,
        current_timestamp as SF_INSERT_DTTM,
        current_timestamp as SF_UPDATE_DTTM
        from user