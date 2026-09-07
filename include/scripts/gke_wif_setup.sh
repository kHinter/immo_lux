set -e

echo $(pwd)

gcloud container clusters get-credentials "$1" --region "$2" --project "$3"

sudo apt-get install google-cloud-cli-gke-gcloud-auth-plugin

kubectl create serviceaccount athome-scraper-ksa --namespace default --dry-run=client -o yaml | kubectl apply -f -

gcloud iam service-accounts add-iam-policy-binding \
  "$4" \
  --role roles/iam.workloadIdentityUser \
  --member "serviceAccount:lux-immo-438316.svc.id.goog[default/athome-scraper-ksa]"

kubectl annotate serviceaccount athome-scraper-ksa \
--namespace default \
iam.gke.io/gcp-service-account="$4" \
--overwrite