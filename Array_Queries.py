def array_queries (N, M, A, B, Q, queries):
    # Write your code here
    num = 998244353
    def compute_F(A, B,N = N, M = M):
        s = 0
        f_list = [(A[i] * B[j]) * (i+j+2) for i in range(N) for j in range(M)] 
        return str(sum(f_list))
    l1 = [None] * (Q+1)
    l1[0] = compute_F(A,B)
    for n in range(Q):

        query_no, i, j = queries[n]
        i -= 1
        j -= 1

        if query_no == 1:
            x = A[i]
            A[i] = B[j]
            B[j] = x
        elif query_no == 2:
            x = A[i]
            A[i] = A[j]
            A[j] = x
        else:
            x = B[i]
            B[i] = B[j]
            B[j] = x
        l1[n+1] = compute_F(A,B)
    return l1
    
        
T = int(input())
for _ in range(T):
    N = int(input())
    M = int(input())
    A = list(map(int, input().split()))
    B = list(map(int, input().split()))
    Q = int(input())
    queries = [list(map(int, input().split())) for i in range(Q)]

    out_ = array_queries(N, M, A, B, Q, queries)
    print (' '.join(map(str, out_)))