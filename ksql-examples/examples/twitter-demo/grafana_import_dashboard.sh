curl -X "POST" "http://localhost:3000/api/dashboards/db" \
     -H "Content-Type: application/json" \
     --user admin:admin \
     --data-binary @twitter_dashboard.json
