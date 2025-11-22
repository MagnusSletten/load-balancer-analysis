import matplotlib.pyplot as plt
import numpy as np

lc_caddy= [13.400, 13.479, 12.742, 13.309, 13.376,
12.795, 13.370, 13.440, 12.769, 13.291]

lc_nginx = [13.632, 13.686, 13.182, 13.811, 13.784,
13.406, 13.683, 13.746, 13.257, 13.891]

lc_traefic = [16.718, 19.029, 16.333, 17.857, 17.206, 16.455, 16.918, 19.026, 16.776, 17.903]

rr_caddy = [17.488, 20.651, 21.102, 19.109, 19.950,19.123, 21.006, 25.718, 22.189, 22.112]


plt.boxplot([lc_caddy,lc_nginx,lc_traefic,rr_caddy],tick_labels=["Caddy LC", "Nginx LC", "Traefik LT", "Caddy Random"])
plt.ylabel("Batch processing time (s)")
plt.title("Batch Processing Time Distribution")
plt.show()



jobs = ["A", "B", "C"]

lc_caddy_med = [2.102, 1.030, 0.515]
lc_nginx_med = [2.098, 1.041, 0.511]
lc_traefic_med = [2.268,1.126,0.589]


x = np.arange(len(jobs))
width = 0.35
spacing = 1.8              
x = np.arange(len(jobs)) * spacing
width = 0.25

plt.figure(figsize=(7,4))

plt.bar(x - width, lc_caddy_med,   width, label="Caddy LC")
plt.bar(x,         lc_nginx_med,   width, label="Nginx LC")
plt.bar(x + width, lc_traefic_med, width, label="Traefik LT")

plt.xticks(x, jobs)
plt.ylabel("median processing time (s)")
plt.xlabel("job")
plt.title("LC – median processing time per job")
plt.legend()
plt.tight_layout()
plt.show()



jobs = ["L", "M", "S"]

lc_caddy_p95 = [2.171, 1.100, 0.578]
lc_nginx_p95 = [2.170, 1.090, 0.571]
lc_traefic_p95 = [4.498,3.343,2.662]

lc_caddy_p95   = [2.171, 1.100, 0.578]
lc_nginx_p95   = [2.170, 1.090, 0.571]
lc_traefic_p95 = [4.498, 3.343, 2.662]

spacing = 1.8              
x = np.arange(len(jobs)) * spacing
width = 0.25

plt.figure(figsize=(7,4))

plt.bar(x - width, lc_caddy_p95,   width, label="Caddy LC")
plt.bar(x,         lc_nginx_p95,   width, label="Nginx LC")
plt.bar(x + width, lc_traefic_p95, width, label="Traefik LT")

plt.xticks(x, jobs)
plt.ylabel("95-percentile processing time (s)")
plt.xlabel("Job")
plt.title("LC – 95-percentile processing time per job")
plt.legend()
plt.tight_layout()
plt.show()

jobs = ["L", "M", "S"]

lc_caddy_p95 = [2.171, 1.100, 0.578]
rr_caddy_p95 = [4.486, 4.190, 2.688]

x = np.arange(len(jobs))
width = 0.35

plt.figure(figsize=(6,4))
plt.bar(x - width/2, lc_caddy_p95, width, label="LC (Caddy)")
plt.bar(x + width/2, rr_caddy_p95, width, label="Random (Caddy)")

plt.xticks(x, jobs)
plt.ylabel("95-percentile processing time (s)")
plt.xlabel("job")
plt.title("LC vs Random (Caddy) – 95-percentile per job")
plt.legend()
plt.tight_layout()
plt.show()


