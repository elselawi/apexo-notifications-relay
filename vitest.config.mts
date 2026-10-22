import { defineWorkersConfig } from '@cloudflare/vitest-pool-workers/config';

// Test-only RSA private key (PKCS#8). It is not a real credential; it exists
// solely so the Google OAuth JWT signing path can be exercised in tests.
const TEST_PRIVATE_KEY = `-----BEGIN PRIVATE KEY-----
MIIEvgIBADANBgkqhkiG9w0BAQEFAASCBKgwggSkAgEAAoIBAQDGw3kmtixinyyC
aKAPF7h5OEU+CcW8MsPAQ+wXWk+/SxFLXLhD4Zew4i/QW1lJ7Gvu6urjvdimVgZc
nFdisCCNu9sJ/edul8lGDkhQqcoLDFf8bwlfdH/rSomIhFnIwUbs0r9T+su6s4Dp
Um7++wQC/oJTrclRJlPEvi/d/EY8gIbwMPjyr+N2wPuo7gt2vC37Rwh8eiB5YUWn
HacSO1cilBl7I90S4E/jq2GwRenPa0O6oVd4wlQxcGdcRPjNGuxMmCjnftDc1mDi
9bkZpD3lFybQS9kGQoDCfqGw/bLxcR7BUMOTrqbOBngD3VBDeVtVjDJdJq8w+kEe
XXwcVCaBAgMBAAECggEAEeTPJ1kHLem+jA73OeScyuzx7t/aWVMIpGp4+rE6tLIB
YJN8U0Af8+CpBUz0AN+z59JmSxJwSqk78WJxy4SnqZrb4U5mY2Ju0EvA3uIqOkxm
L2efpGRMj9YqA4xGuPgNdoyShPDzHl/Cnz64l2PpvItFGFtw4ElfCTtF++oWOKQA
42nuOo+gpG/sC64Yigw03ue6Pfeeg96WsXJS4dwdrr8JovPid0oGj1nJH6dImPzD
uN+D5YCd9mwHiCe8cwIcxb0wt/Ugb1Tst6Gc0bVONA+CdFUz0oIhCWmbtXPhHqhS
ghabWKqXlr6qnzvNZ8TWCxseyAqmJs375v5kHrb7MQKBgQDkpaAUgxH2R6Gh9hJ4
TmkB4K7d/fUYgfnWUm6N2bvqeJ5sV+dOvZod1LDcmh2iZDz2sbOWNUizHCddJfd1
Xa6+u7s8NLB5XDZfxPPGZ+F4MZ3XY/stZTvoXZcIq5DsFExeOhuzBWwJVhBRlIG5
EG9+kwUwJ7N+ZloVNGypab/QVwKBgQDeiqkejpp9aLg8NfnDv3Zzphwx4jxJ8O84
fAmupbXEtTWz/mA/ZkL9wec79KSJXAWoQYrK8FFi/ZhFj7e0m2U+wTNsj9ctIlLE
scdwYXv2wlNdfP3gOuNh6pqpjNGtPZR5Q6TyvCtY2srf4zCxmVddXq4msU8rLK5J
M/N3AqEY5wKBgQCBIb1P+Fprojb59rSlt3r8sX1xKJ2E0ttcHMANYvwLR0YJ4Hdl
dBWXZWjhnjTAYyY/GUtYuQ0vAuaEaA3i/h7T38i2DvfgAqE+40feNQsUy8jePbw8
/iCgckL7hZHakvgye1TrfVNvzmh2+o1tDIg7jyLt+G4aJAoaUbpQz8WiDwKBgHwF
FGjWGhynvny3q+C+E9oxiWb3aJfgcMCux4qQCPN99VbA/Yx3hf/7Ds2n8gYmxXRW
Sm38ba7YGwMxK7S4IT1O2ES5CLa7eRrq5HWprjb/Zwwjuyw6RTvtTZINy1BbkCHg
Y81utk2HaSZKa3meNkTvnegsSi2KcfqTvCvnyDWjAoGBALBITlpyg8kPu/Je5GFj
kuhQCBIDbdrcEmJ3sHq/z101Y7rdgIYo++IXqDrWsuMW3WJgEzlcgLwAL58tcR96
mZffDPLLqbc0fxN0A4FqnT6KYKLzlo0tkC7lKyJpnSNN4eM+1tSKuzEqvGiG4ZI8
G/L9iJN8ndxUU0PPaHrpgNVE
-----END PRIVATE KEY-----`;

export default defineWorkersConfig({
	test: {
		poolOptions: {
			workers: {
				wrangler: { configPath: './wrangler.jsonc' },
				miniflare: {
					bindings: {
						FIREBASE_SERVICE_ACCOUNT: JSON.stringify({
							type: 'service_account',
							project_id: 'test-project',
							client_email: 'test@test-project.iam.gserviceaccount.com',
							private_key: TEST_PRIVATE_KEY,
							token_uri: 'https://oauth2.googleapis.com/token',
						}),
						WNS_TENANT_ID: 'test-tenant',
						WNS_CLIENT_ID: 'test-client-id',
						WNS_CLIENT_SECRET: 'test-client-secret',
					},
				},
			},
		},
	},
});
