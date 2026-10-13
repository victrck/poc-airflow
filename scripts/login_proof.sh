#!/bin/bash
# Prova de RBAC por login real (OAuth2 /auth/token + JWT na API pública).
# Uso: ./scripts/login_proof.sh
# Cria 1 run do financeiro_dummy (inofensivo) e 1 variable temporária (removida).
set -u
cd "$(dirname "$0")"
BASE=${BASE:-http://localhost:8081}
pass=0; fail=0
token() { curl -sf "$BASE/auth/token" -X POST -H 'Content-Type: application/json' -d "{\"username\":\"$1\",\"password\":\"$2\"}" | python3 -c "import json,sys; print(json.load(sys.stdin)['access_token'])"; }
check() { # check <desc> <expected> <actual>
  if [ "$2" = "$3" ]; then echo "OK   $1 (=$2)"; pass=$((pass+1)); else echo "FAIL $1 esperado=$2 obtido=$3"; fail=$((fail+1)); fi
}
T_READ=$(token usr_read kolmeia_read123); T_WRITE=$(token usr_write kolmeia_write123); T_OPS=$(token usr_ops kolmeia_ops123)

check "read lista DAGs" 200 "$(curl -s -o /dev/null -w '%{http_code}' -H "Authorization: Bearer $T_READ" $BASE/api/v2/dags?limit=100)"
echo "$T_READ" | python3 -c "pass" 2>/dev/null
N_DAGS=$(curl -sf -H "Authorization: Bearer $T_READ" "$BASE/api/v2/dags?limit=100" | python3 -c "import json,sys; print(len(json.load(sys.stdin)['dags']))")
check "read enxerga 5 DAGs" 5 "$N_DAGS"
check "read trigger -> 403" 403 "$(curl -s -o /dev/null -w '%{http_code}' -H "Authorization: Bearer $T_READ" -X POST $BASE/api/v2/dags/financeiro_dummy/dagRuns -H 'Content-Type: application/json' -d '{}')"
check "read variables -> 403" 403 "$(curl -s -o /dev/null -w '%{http_code}' -H "Authorization: Bearer $T_READ" $BASE/api/v2/variables)"
NOW=$(date -u +%Y-%m-%dT%H:%M:%S+00:00)
check "write trigger -> 200" 200 "$(curl -s -o /dev/null -w '%{http_code}' -H "Authorization: Bearer $T_WRITE" -X POST $BASE/api/v2/dags/financeiro_dummy/dagRuns -H 'Content-Type: application/json' -d "{\"conf\":{},\"logical_date\":\"$NOW\"}")"
check "write variables -> 403" 403 "$(curl -s -o /dev/null -w '%{http_code}' -H "Authorization: Bearer $T_WRITE" $BASE/api/v2/variables)"
check "ops variables GET" 200 "$(curl -s -o /dev/null -w '%{http_code}' -H "Authorization: Bearer $T_OPS" $BASE/api/v2/variables)"
check "ops cria variable" 201 "$(curl -s -o /dev/null -w '%{http_code}' -H "Authorization: Bearer $T_OPS" -X POST $BASE/api/v2/variables -H 'Content-Type: application/json' -d '{"key":"rbac_proof_tmp","value":"x"}')"
check "ops apaga variable" 204 "$(curl -s -o /dev/null -w '%{http_code}' -H "Authorization: Bearer $T_OPS" -X DELETE $BASE/api/v2/variables/rbac_proof_tmp)"
echo "== $pass OK, $fail FAIL =="
[ "$fail" -eq 0 ] && echo LOGIN-PROOF-OK || exit 1
