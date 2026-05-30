# Next.js observability UI. NEXT_PUBLIC_* are inlined into client JS at build
# time and must point at browser-reachable ingresses: the poorbricks-server API
# and (for the Ask/Docs/Glossary/Memory tabs) Cosmo's knowledge API.
FROM node:18-slim AS builder
WORKDIR /app
COPY package.json package-lock.json ./
RUN npm ci
COPY . .
ARG NEXT_PUBLIC_API_URL
ENV NEXT_PUBLIC_API_URL=$NEXT_PUBLIC_API_URL
ARG NEXT_PUBLIC_COSMO_URL
ENV NEXT_PUBLIC_COSMO_URL=$NEXT_PUBLIC_COSMO_URL
RUN npm run build

FROM node:18-slim AS runner
WORKDIR /app
ENV NODE_ENV=production
ENV PORT=3000
ENV HOSTNAME=0.0.0.0
COPY --from=builder /app/.next/standalone ./
COPY --from=builder /app/.next/static ./.next/static
EXPOSE 3000
CMD ["node", "server.js"]
