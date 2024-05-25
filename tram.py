def solve (N, start, finish, Ticket_cost):
    # Write your code here
    start -= 1
    finish -= 1
    if start == finish:
        return 0
    if start < finish:
        clockwise_cost = sum(Ticket_cost[start:finish])
        counter_clock = sum(Ticket_cost[:start]) + sum(Ticket_cost[finish:])
    else:
        clockwise_cost = sum(Ticket_cost[:finish]) + sum(Ticket_cost[start:])
        counter_clock = sum(Ticket_cost[finish:start])      
    return min(clockwise_cost, counter_clock)  
    

N = int(input())
start = int(input())
finish = int(input())
Ticket_cost = list(map(int, input().split()))

out_ = solve(N, start, finish, Ticket_cost)
print (out_)