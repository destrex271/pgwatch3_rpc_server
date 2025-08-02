package sinks

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/destrex271/pgwatch3_rpc_server/sinks/pb"
	testutils "github.com/destrex271/pgwatch3_rpc_server/sinks/test_utils"
	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

const PlainServerPort = "5050"
const PlainServerAddress = "localhost:5050" // CN in test cert is `localhost`
const TLSServerPort = "6060"
const TLSServerAddress = "localhost:6060"

const TestCertFile = "server.crt"
const TestPrivateKeyFile = "server.key"

type Sink struct {
	SyncMetricHandler
}

func (s *Sink) UpdateMeasurements(ctx context.Context, msg *pb.MeasurementEnvelope) (*pb.Reply, error) {
	return &pb.Reply{Logmsg: "Measurements Updated"}, nil
}

func NewSink() *Sink {
	return &Sink{
		SyncMetricHandler: NewSyncMetricHandler(1024),
	}
}

type Writer struct {
	client pb.ReceiverClient
}

func NewRPCWriter(withTLS bool) *Writer {
	var creds credentials.TransportCredentials
	var address string

	if withTLS {
		certPool := x509.NewCertPool()
		certPool.AppendCertsFromPEM(TestCA)
		tlsClientConfig := &tls.Config{
			RootCAs: certPool,
		}

		creds = credentials.NewTLS(tlsClientConfig)
		address = TLSServerAddress
	} else {
		creds = insecure.NewCredentials()
		address = PlainServerAddress
	}

	conn, err := grpc.NewClient(address, grpc.WithTransportCredentials(creds))
	if err != nil {
		panic(err)
	}

	client := pb.NewReceiverClient(conn)
	return &Writer{
		client: client,
	}
}

func (w *Writer) WriteWithCreds(msg *pb.MeasurementEnvelope, username, password string) (*pb.Reply, error) {
	md := metadata.Pairs(
		"username", username,
		"password", password,
	)
	ctx := metadata.NewOutgoingContext(context.Background(), md)

	reply, err := w.client.UpdateMeasurements(ctx, msg)	
	return reply, err
}

var writer *Writer

func TestMain(m *testing.M) {
	err := os.WriteFile(TestCertFile, TestCert, 0644)
	if err != nil {
		panic(err)
	}

	err = os.WriteFile(TestPrivateKeyFile, TestPrivateKey, 0644)
	if err != nil {
		panic(err)
	}

	serversMetadata := map[string][2]string{
		PlainServerPort: {},
		TLSServerPort: {TestCertFile, TestPrivateKeyFile},
	}

	receiver := NewSink()
	for port, tlsFiles := range serversMetadata {
		SERVER_CERT = tlsFiles[0]
		SERVER_KEY = tlsFiles[1]

		go func () {
			err := ListenAndServe(receiver, port)	
			if err != nil {
				panic(err)
			}
		}()
		time.Sleep(time.Second)
	}
	writer = NewRPCWriter(false)

	exitCode := m.Run()

	_ = os.Remove(TestCertFile)
	_ = os.Remove(TestPrivateKeyFile)
	os.Exit(exitCode)
}

// Tests begin from here

func Test_gRPCServer(t *testing.T) {
	msg := testutils.GetTestMeasurementEnvelope()
	req := testutils.GetTestRPCSyncRequest()

	TLSWriter := NewRPCWriter(true)
	writers := [2]*Writer{writer, TLSWriter}

	for _, w := range writers {
		reply, err := w.client.UpdateMeasurements(context.Background(), msg)
		assert.NoError(t, err, "error calling UpdateMeasurements()")
		assert.Equal(t, reply.GetLogmsg(), "Measurements Updated")

		reply, err = w.client.SyncMetric(context.Background(), req)
		assert.NoError(t, err, "error calling SyncMetric()")
		assert.Equal(t, fmt.Sprintf("gRPC Receiver Synced: DBName %s MetricName %s Operation %s", req.GetDBName(), req.GetMetricName(), "Add"), reply.GetLogmsg()) 
	}	
}

func TestMsgValidationInterceptor(t *testing.T) {
	msg := &pb.MeasurementEnvelope{}

	reply, err := writer.client.UpdateMeasurements(context.Background(), msg)
	assert.ErrorIs(t, err, status.Error(codes.InvalidArgument, "empty database name"))
	assert.Nil(t, reply)
}

func TestAuthInterceptor(t *testing.T) {
	msg := testutils.GetTestMeasurementEnvelope()

	serverCreds := []string{
		",", 
		"username,password",
		"username,", 
		",password",
	}

	validCreds := [][]string{
		{",", "username,password", "username,", ",password"},
		{"username,password"},
		{"username,", "username,random_password"},
		{",password", "random_username,password"},
	}

	invalidCreds := [][]string{
		{},
		{",password", "username,", "notusername,password", "username,notpassword", "notusername,notpassword"},
		{"notusername,", "notusername,random_password"},
		{",notpassword", "random_username,notpassword"},
	}

	for i, serverCred := range serverCreds {
		SERVER_USERNAME, SERVER_PASSWORD, _ = strings.Cut(serverCred, ",")

		for _, validCred := range validCreds[i] {
			clientUsername, clientPassword, _ := strings.Cut(validCred, ",")
			reply, err := writer.WriteWithCreds(msg, clientUsername, clientPassword)
			assert.NoError(t, err)
			assert.Equal(t, reply.GetLogmsg(), "Measurements Updated")
		}


		for _, invalidCred := range invalidCreds[i] {
			clientUsername, clientPassword, _ := strings.Cut(invalidCred, ",")
			reply, err := writer.WriteWithCreds(msg, clientUsername, clientPassword)
			assert.Error(t, err, status.Error(codes.Unauthenticated, "invalid username or password"))
			assert.Nil(t, reply)
		}
	}
}


// End of tests

var TestCA = []byte(`-----BEGIN CERTIFICATE-----
MIIDPzCCAiegAwIBAgIUeENQlQFVH5h7HszFJLLWo+KCQwQwDQYJKoZIhvcNAQEL
BQAwEjEQMA4GA1UEAwwHcGd3YXRjaDAeFw0yNTA2MTEwNTI1MDJaFw0zNTA2MDkw
NTI1MDJaMBIxEDAOBgNVBAMMB3Bnd2F0Y2gwggEiMA0GCSqGSIb3DQEBAQUAA4IB
DwAwggEKAoIBAQDtTW+kyvb3Y5OaYlriKp8HkHt95lfOgxQNZQfiREfEyLWU59bx
0ZIvFmejK38Qc0dlca9d+5tEkxotsbggJflLljfmnzhxsuZpr8SjmDd1m8XSo0IA
oDlVbKO6SZMlsyq3QrAOYAjG1LTPlATqvGAOs9NfFonjwoPXCjIwSfa+wexe5dRD
gJ114AKXw3ck5ZQ4Pw+w5ylgNSfVl548WY9DSOA+6HlZ17MYA1qmMOTwKae5fmsc
xlRPoIV3EJrKas7VlbebDOXOSXDV+9aMW6ox1xanUUDUgabzkzntfmuOttaIX1g1
nSMuHa7EEQF7lxgdg9OU8i/jygdlGcgBUYjtAgMBAAGjgYwwgYkwDAYDVR0TBAUw
AwEB/zAdBgNVHQ4EFgQUKGJs7OXINd2WL6X2meH90eINoJQwTQYDVR0jBEYwRIAU
KGJs7OXINd2WL6X2meH90eINoJShFqQUMBIxEDAOBgNVBAMMB3Bnd2F0Y2iCFHhD
UJUBVR+Yex7MxSSy1qPigkMEMAsGA1UdDwQEAwIBBjANBgkqhkiG9w0BAQsFAAOC
AQEABdY/4rsgMu+sCqEdacNzHqAz9X1ew37y1UONngm/7LPqbQrzzg/fBvOOJLcd
IzMJPtpdwokPYOW29jw/hY4R1RWr8012zc8Z0GsuDR7I/Z2Hww7tzYhf1H5mjy1d
eQDhHNpsSb5pHLoPft5O0sT/0WqAlKWPb2KmSoAio8jE2BSUTK3ZgE0yJIikONon
HCWOlNCWx+RsyPoRnQqbpVa+SmGBqpiyHchpZ8sFPe+pgPu+8u921lJ0PRvmfp7L
4YZIaM8LQAV8FWk2VLXmsqYUJYYLAXCG6Unkx1oIOtq1AyAoXHCl/3hKbCeXIrgA
Cs5qN+ZUHRdKff5gFpraKtHKkw==
-----END CERTIFICATE-----`)

var TestCert = []byte(`-----BEGIN CERTIFICATE-----
MIIDZTCCAk2gAwIBAgIQeTQ+4M7xwydf7MvrDnDdsTANBgkqhkiG9w0BAQsFADAS
MRAwDgYDVQQDDAdwZ3dhdGNoMB4XDTI1MDYxMTA1MjUxOFoXDTI3MDkxNDA1MjUx
OFowFDESMBAGA1UEAwwJbG9jYWxob3N0MIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8A
MIIBCgKCAQEA69Ja9K4ZHoIdBK+34YE1+0a/sB7YKZJb3+gYFahvszS37Oa6h/0+
d9TCY58bMpPQSQQdvhG/s1H6Yc4BWTfH/ssRmDhKciMmdfMj/lr8TytglIUPPSzb
PKy2t9idpk30PwiV1UlijjlFZxoxcO3Aia8mmrDJzkoHsQL96PhDT25YRinnflg8
vVthasVqGIHNJIXORyz5lgkBW3NeeZPEUSxbvmo63AB1lFJZMz4xlpdN/LVsEOwg
FrrYzb4mpGkAcTenkOfU4W7m7bxpsusW2JDm2O8bsx+v3cazOqJpCLMHP3Vqnzcl
loYmeQauBep6H0wspid0YzMVza75pDx7rQIDAQABo4G0MIGxMAkGA1UdEwQCMAAw
HQYDVR0OBBYEFFcDjPaZMelQIueauIRfo4L4wMehME0GA1UdIwRGMESAFChibOzl
yDXdli+l9pnh/dHiDaCUoRakFDASMRAwDgYDVQQDDAdwZ3dhdGNoghR4Q1CVAVUf
mHsezMUkstaj4oJDBDATBgNVHSUEDDAKBggrBgEFBQcDATALBgNVHQ8EBAMCBaAw
FAYDVR0RBA0wC4IJbG9jYWxob3N0MA0GCSqGSIb3DQEBCwUAA4IBAQCM6tYNxoP2
Gbp3aAPjoA3+U1gWHPHXOOgyhaQw4jJ7xK1MUlrFgSG6cJgO7IRSCIZp7GDZmIjo
+PqWRgMNK2pFCUCqjrAV6NwMjApLzDdSza9xKb3nWXMKnV6j3tNUFUCS68CHAM7Q
E1iuepjIy2VReFfjJoPuhp9OQBWobTo3H9F74Sj+Guu0lDcHWbwn5Y92pnKk0vOh
v1AJ6vwdMpd6DAPlwmY3OcZI2FGYyoPP2CnzHIGP5RoVFp1zkJzoFvnOHnsRMByz
HpGKqYFQVJSAOMCtL2OMiP8MxtiCsdz6j/e3/VOUQuYoM6fXFhZO64xekZdlh/ZR
glsaMXQPWvHX
-----END CERTIFICATE-----`)

var TestPrivateKey = []byte(`-----BEGIN PRIVATE KEY-----
MIIEvgIBADANBgkqhkiG9w0BAQEFAASCBKgwggSkAgEAAoIBAQDr0lr0rhkegh0E
r7fhgTX7Rr+wHtgpklvf6BgVqG+zNLfs5rqH/T531MJjnxsyk9BJBB2+Eb+zUfph
zgFZN8f+yxGYOEpyIyZ18yP+WvxPK2CUhQ89LNs8rLa32J2mTfQ/CJXVSWKOOUVn
GjFw7cCJryaasMnOSgexAv3o+ENPblhGKed+WDy9W2FqxWoYgc0khc5HLPmWCQFb
c155k8RRLFu+ajrcAHWUUlkzPjGWl038tWwQ7CAWutjNviakaQBxN6eQ59Thbubt
vGmy6xbYkObY7xuzH6/dxrM6omkIswc/dWqfNyWWhiZ5Bq4F6nofTCymJ3RjMxXN
rvmkPHutAgMBAAECggEAAc8djQJ35VzEqbhKXhO+bQTMLCb0bA84HrXaV3IxFywY
nBviAvCNpeAvNJHwJLlvD9xU+RQMRy0iEVWB+6P6qAj5Q9Rst8buwNliZY1foaDY
zxLdPNAnB2ZgyXTDMtcmwEQJ2DbFp4cnceTIy8+7GiNKlcW06pz1RaWa+opLA+U2
STIxvTEAvqsyE/0KHbeEltwZxeZ83BsX8vhpyrCVvniFJnIMvyYG7iTzLWuTK98Z
R3Baqim8CdWbh2W0OOfphAVlTjG6c0r6FqJIqsds9wf2FfuhgUcNQUXke+uuWbPQ
36RsytymUgqye3DkxrC4dEi27S3cjRh5wK53gqET+QKBgQD8zM5wiindvDRSx+66
ppbl6RJQcL7uv1os9TeNvfqwnhC75y2k4+2s8kiG2ik7aJiRg4dkj3rVJ4S6wQ67
sjRVo5z68J1twP6PqvpJyx/G5Fmy8HPJUmy9FM1AdnYCWGX3bh3qNTkxEVn78yFZ
zfn9CczDAmGErAXPRRDNQQy8ZQKBgQDuzofs5EXTpqw25XveCJKHT3fAis7pkNGu
jwopiR9peKuJ9nNarHH2wWpHRn6zgHmhmAu7oEzo4OEmk4Elo5ffqqRPZZaU8aEo
Ow7cRvedoP/EjJaR8m2uQnh9bWXuEVibfKmkrPswYYUCWmwoALFmzd6Gl0dIdyGk
JXeA/jFZqQKBgQDhFbn5mgsM0rYDvuBgcFOLAaq81KYsDVRNE0kTe0PqXdKoe324
gvjsNA0/hJ+Rtd+iMGosr1O+1iDn510m4dSXK8Zp6DNDtcLySFnxulngzRDQsidl
6W3ILO1TqCYKkIq5c+JO1nTFq51jJ2dafntHQaJ/P290oXXKxsPe/TxJwQKBgD6g
dS8f8lv+Mt22sxRYhSztH0ekX30LWKIBqzWXW2CKn9nvgvL9lGmU8a09hI7Im51Q
RYtwD5tnFkTKnCzlyTeEBdE4oBPxhkUJr+z+w4NYLJs8D2S5AiCYGAc0wG19qRIl
0Et6femDOaGTWxfmjp+aT8hWNgCAFZd5p+xxPTn5AoGBAIeUbCRTe0KcVaKw/Lcj
KHjTih9x9d3f2EbnYbziBZz6fZfWdDIBfA6CbIHil21hNvjDm9Yci5b6FgtUzScb
j3vPa99sMGc2xie07Cd7LTvZeWIVXeW1Dxzex89CqoJzmONrco1ZKQ+xXbPoZCBj
VwAI/SWm7NlxgF6Sr5CIo2KR
-----END PRIVATE KEY-----`)