# concurrent-booking
System design reading is boring, let's build
1. create data base.
2. add mock data
3. can use setup.sql
4. add version in table.
5. go run main.go concurrency_control.go
6. use api
    1. for booking with different method.
    2. find the status of existing.
    3. do payment.
    4. cache removal or time out for seat is 1 min, after that if not able to pay then seat will be available for others.
