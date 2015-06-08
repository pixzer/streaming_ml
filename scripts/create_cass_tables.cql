 insert into sparkml.accuracy (event_time, mse, rmse) values (now(), 597.89, 25.34) ;
 insert into sparkml.accuracy (event_time, mse, rmse) values (now(), 0.0,0.0,)
 
 create table sparkml.accuracy ( event_time timeuuid, mse float, rmse float, primary key (mse, event_time)  )  with clustering order by (event_time desc);
 