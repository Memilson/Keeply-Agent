#include "ws.hpp"
#include "multithread.hpp"
#include <arpa/inet.h>
#include <netdb.h>
#include <sys/socket.h>
#include <unistd.h>
#include <openssl/evp.h>
#include <openssl/pem.h>
#include <openssl/sha.h>
#include <openssl/ssl.h>
#include <openssl/x509.h>
#include <algorithm>
#include <array>
#include <atomic>
#include <cerrno>
#include <chrono>
#include <cstdint>
#include <ctime>
#include <cstdio>
#include <cstring>
#include <fstream>
#include <functional>
#include <iostream>
#include <limits>
#include <map>
#include <random>
#include <sstream>
#include <stdexcept>
#include <thread>
#include <unordered_map>
#include <vector>

namespace {

struct ParsedUrl{std::string scheme;std::string host;int port=0;std::string target="/";};
struct HttpResponse{int status=0;std::string body;};
struct MultipartField{std::string name;std::string value;};
struct UploadBundleResult{
    HttpResponse manifestResponse;
    std::size_t filesUploaded=0;
    std::size_t blobPartCount=0;
    std::string bundleId;
};

struct UploadProgressSnapshot{
    std::size_t filesUploaded=0;
    std::size_t filesTotal=0;
    std::size_t blobPartIndex=0;
    std::size_t blobPartCount=0;
    std::string currentObject;
    std::string bundleRole;
};

struct BackupStoragePolicy{
    std::string mode="local_only";
    bool keepLocal=true;
    bool uploadCloud=false;
    bool deleteLocalAfterUpload=false;
};

std::string toLower(std::string value){
    for(char& c:value) if(c>='A'&&c<='Z') c=(char)(c-'A'+'a');
    return value;
}

std::string hexEncode(const unsigned char* data,std::size_t size){
    static const char* h="0123456789abcdef";
    std::string out;out.resize(size*2);
    for(std::size_t i=0;i<size;++i){out[i*2]=h[(data[i]>>4)&0x0F];out[i*2+1]=h[data[i]&0x0F];}
    return out;
}

std::string base64Encode(const unsigned char* data,std::size_t size){
    std::string out;out.resize(((size+2)/3)*4);
    const int written=EVP_EncodeBlock(reinterpret_cast<unsigned char*>(&out[0]),data,(int)size);
    if(written<0) throw std::runtime_error("Falha ao codificar base64.");
    out.resize((std::size_t)written);
    return out;
}

std::string randomBase64(std::size_t bytes){
    std::vector<unsigned char> raw(bytes);
    std::random_device rd;
    for(std::size_t i=0;i<bytes;++i) raw[i]=(unsigned char)rd();
    return base64Encode(raw.data(),raw.size());
}

std::string randomDigits(std::size_t digits){
    std::random_device rd;
    std::uniform_int_distribution<int> dist(0,9);
    std::string out;out.reserve(digits);
    for(std::size_t i=0;i<digits;++i) out.push_back((char)('0'+dist(rd)));
    return out;
}

ParsedUrl parseUrlCommon(const std::string& url){
    const std::size_t schemeSep=url.find("://");
    if(schemeSep==std::string::npos) throw std::runtime_error("URL invalida: esquema ausente.");
    ParsedUrl parsed;
    parsed.scheme=toLower(url.substr(0,schemeSep));
    const std::string rest=url.substr(schemeSep+3);
    const std::size_t slash=rest.find('/');
    const std::string hostPort=slash==std::string::npos?rest:rest.substr(0,slash);
    parsed.target=slash==std::string::npos?"/":rest.substr(slash);
    if(hostPort.empty()) throw std::runtime_error("URL invalida: host ausente.");
    const std::size_t colon=hostPort.rfind(':');
    if(colon!=std::string::npos&&hostPort.find(':')==colon){
        parsed.host=hostPort.substr(0,colon);
        parsed.port=std::stoi(hostPort.substr(colon+1));
    }else{
        parsed.host=hostPort;
        if(parsed.scheme=="ws"||parsed.scheme=="http") parsed.port=80;
        else if(parsed.scheme=="wss"||parsed.scheme=="https") parsed.port=443;
        else throw std::runtime_error("Esquema de URL nao suportado: "+parsed.scheme);
    }
    if(parsed.host.empty()||parsed.port<=0||parsed.port>65535) throw std::runtime_error("URL invalida.");
    return parsed;
}

std::string urlEncode(const std::string& value){
    static const char* kHex="0123456789ABCDEF";
    std::string out;
    for(unsigned char c:value){
        const bool safe=(c>='a'&&c<='z')||(c>='A'&&c<='Z')||(c>='0'&&c<='9')||c=='-'||c=='_'||c=='.'||c=='~';
        if(safe) out.push_back((char)c);
        else{out.push_back('%');out.push_back(kHex[(c>>4)&0x0F]);out.push_back(kHex[c&0x0F]);}
    }
    return out;
}

std::string escapeJson(const std::string& value){
    std::string out;out.reserve(value.size()+8);
    for(char c:value){
        switch(c){
            case '\"': out+="\\\""; break;
            case '\\': out+="\\\\"; break;
            case '\b': out+="\\b"; break;
            case '\f': out+="\\f"; break;
            case '\n': out+="\\n"; break;
            case '\r': out+="\\r"; break;
            case '\t': out+="\\t"; break;
            default: if((unsigned char)c<0x20) out+="?"; else out+=c;
        }
    }
    return out;
}

int openTcpSocket(const std::string& host,int port){
    addrinfo hints{};hints.ai_family=AF_UNSPEC;hints.ai_socktype=SOCK_STREAM;
    addrinfo* result=nullptr;
    const std::string portStr=std::to_string(port);
    const int rc=::getaddrinfo(host.c_str(),portStr.c_str(),&hints,&result);
    if(rc!=0) throw std::runtime_error(std::string("getaddrinfo falhou: ")+gai_strerror(rc));
    int fd=-1;
    for(addrinfo* p=result;p!=nullptr;p=p->ai_next){
        fd=::socket(p->ai_family,p->ai_socktype,p->ai_protocol);
        if(fd<0) continue;
        if(::connect(fd,p->ai_addr,p->ai_addrlen)==0) break;
        ::close(fd);fd=-1;
    }
    ::freeaddrinfo(result);
    if(fd<0) throw std::runtime_error("Nao foi possivel conectar ao backend.");
    return fd;
}

struct HttpTls{
    SSL_CTX* ctx=nullptr;
    SSL* ssl=nullptr;
    ~HttpTls(){if(ssl){SSL_shutdown(ssl);SSL_free(ssl);}if(ctx) SSL_CTX_free(ctx);}
};

std::size_t writeAllFd(int fd,const void* data,std::size_t size){
    const unsigned char* ptr=(const unsigned char*)data;
    std::size_t offset=0;
    while(offset<size){
        int flags=0;
#ifdef MSG_NOSIGNAL
        flags|=MSG_NOSIGNAL;
#endif
        const ssize_t n=::send(fd,ptr+(std::ptrdiff_t)offset,size-offset,flags);
        if(n<0){if(errno==EINTR) continue;throw std::runtime_error(std::string("send falhou: ")+std::strerror(errno));}
        offset+=(std::size_t)n;
    }
    return offset;
}

std::size_t writeAllSsl(SSL* ssl,const void* data,std::size_t size){
    const unsigned char* ptr=(const unsigned char*)data;
    std::size_t offset=0;
    while(offset<size){
        const int n=SSL_write(ssl,ptr+(std::ptrdiff_t)offset,(int)(size-offset));
        if(n<=0){
            const int err=SSL_get_error(ssl,n);
            if(err==SSL_ERROR_WANT_READ||err==SSL_ERROR_WANT_WRITE) continue;
            throw std::runtime_error("SSL_write falhou.");
        }
        offset+=(std::size_t)n;
    }
    return offset;
}

std::size_t readSomeFd(int fd,void* data,std::size_t size){
    for(;;){
        const ssize_t n=::recv(fd,data,size,0);
        if(n<0){if(errno==EINTR) continue;throw std::runtime_error(std::string("recv falhou: ")+std::strerror(errno));}
        return (std::size_t)n;
    }
}

std::size_t readSomeSsl(SSL* ssl,void* data,std::size_t size){
    for(;;){
        const int n=SSL_read(ssl,data,(int)size);
        if(n>0) return (std::size_t)n;
        const int err=SSL_get_error(ssl,n);
        if(err==SSL_ERROR_WANT_READ||err==SSL_ERROR_WANT_WRITE) continue;
        if(err==SSL_ERROR_ZERO_RETURN) return 0;
        throw std::runtime_error("SSL_read falhou.");
    }
}

HttpTls connectTls(int fd,const ParsedUrl& url,const std::optional<keeply::fs::path>& certPemPath,const std::optional<keeply::fs::path>& keyPemPath,bool allowInsecureTls){
    HttpTls tls;
    tls.ctx=SSL_CTX_new(TLS_client_method());
    if(!tls.ctx) throw std::runtime_error("Falha ao criar SSL_CTX.");
    if(allowInsecureTls){
        SSL_CTX_set_verify(tls.ctx,SSL_VERIFY_NONE,nullptr);
    }else{
        SSL_CTX_set_verify(tls.ctx,SSL_VERIFY_PEER,nullptr);
        if(SSL_CTX_set_default_verify_paths(tls.ctx)!=1) throw std::runtime_error("Falha ao configurar trust store do TLS.");
    }
    if(certPemPath&&keyPemPath){
        if(SSL_CTX_use_certificate_file(tls.ctx,certPemPath->c_str(),SSL_FILETYPE_PEM)!=1) throw std::runtime_error("Falha ao carregar certificado do agente.");
        if(SSL_CTX_use_PrivateKey_file(tls.ctx,keyPemPath->c_str(),SSL_FILETYPE_PEM)!=1) throw std::runtime_error("Falha ao carregar chave privada do agente.");
    }
    tls.ssl=SSL_new(tls.ctx);
    if(!tls.ssl) throw std::runtime_error("Falha ao criar SSL.");
    SSL_set_fd(tls.ssl,fd);
    SSL_set_tlsext_host_name(tls.ssl,url.host.c_str());
    if(SSL_connect(tls.ssl)!=1) throw std::runtime_error("Falha no handshake TLS com o backend.");
    return tls;
}

std::string readHttpResponseBody(int fd,SSL* ssl){
    std::string data;
    char buf[4096];
    for(;;){
        const std::size_t n=ssl?readSomeSsl(ssl,buf,sizeof(buf)):readSomeFd(fd,buf,sizeof(buf));
        if(n==0) break;
        data.append(buf,n);
    }
    return data;
}

HttpResponse httpPostJson(const std::string& url,const std::string& body,const std::optional<keeply::fs::path>& certPemPath,const std::optional<keeply::fs::path>& keyPemPath,bool allowInsecureTls){
    const ParsedUrl parsed=parseUrlCommon(url);
    if(parsed.scheme!="http"&&parsed.scheme!="https") throw std::runtime_error("Enroll HTTP suporta apenas http:// ou https://");
    const int fd=openTcpSocket(parsed.host,parsed.port);
    HttpTls tls;
    SSL* ssl=nullptr;
    try{
        if(parsed.scheme=="https"){tls=connectTls(fd,parsed,certPemPath,keyPemPath,allowInsecureTls);ssl=tls.ssl;}
        std::ostringstream req;
        req<<"POST "<<parsed.target<<" HTTP/1.1\r\n";
        req<<"Host: "<<parsed.host<<":"<<parsed.port<<"\r\n";
        req<<"Content-Type: application/json\r\n";
        req<<"Accept: application/json\r\n";
        req<<"Connection: close\r\n";
        req<<"Content-Length: "<<body.size()<<"\r\n";
        req<<"\r\n";
        req<<body;
        const std::string request=req.str();
        if(ssl) writeAllSsl(ssl,request.data(),request.size()); else writeAllFd(fd,request.data(),request.size());
        std::string raw=readHttpResponseBody(fd,ssl);
        ::close(fd);
        const std::size_t headerEnd=raw.find("\r\n\r\n");
        if(headerEnd==std::string::npos) throw std::runtime_error("Resposta HTTP invalida no enroll.");
        std::istringstream hs(raw.substr(0,headerEnd));
        std::string statusLine;std::getline(hs,statusLine);
        std::istringstream sl(statusLine);
        std::string httpVersion;
        HttpResponse resp;
        sl>>httpVersion>>resp.status;
        resp.body=raw.substr(headerEnd+4);
        return resp;
    }catch(...){
        ::close(fd);
        throw;
    }
}

HttpResponse httpPostMultipartFile(const std::string& url,
                                   const std::vector<MultipartField>& fields,
                                   const std::string& fileFieldName,
                                   const keeply::fs::path& filePath,
                                   const std::string& fileName,
                                   const std::string& fileContentType,
                                   const std::optional<keeply::fs::path>& certPemPath,
                                   const std::optional<keeply::fs::path>& keyPemPath,
                                   bool allowInsecureTls){
    const ParsedUrl parsed=parseUrlCommon(url);
    if(parsed.scheme!="http"&&parsed.scheme!="https") throw std::runtime_error("Upload HTTP suporta apenas http:// ou https://");
    std::error_code fileEc;
    const std::uintmax_t fileSize=keeply::fs::file_size(filePath,fileEc);
    if(fileEc) throw std::runtime_error("Falha lendo tamanho do arquivo para upload: "+fileEc.message());

    const std::string boundary="----KeeplyBoundary"+randomDigits(24);
    std::vector<std::string> fieldParts;
    fieldParts.reserve(fields.size());
    std::uint64_t contentLength=0;
    for(const auto& field:fields){
        std::ostringstream part;
        part<<"--"<<boundary<<"\r\n";
        part<<"Content-Disposition: form-data; name=\""<<field.name<<"\"\r\n\r\n";
        part<<field.value<<"\r\n";
        fieldParts.push_back(part.str());
        contentLength+=fieldParts.back().size();
    }

    std::ostringstream fileHeader;
    fileHeader<<"--"<<boundary<<"\r\n";
    fileHeader<<"Content-Disposition: form-data; name=\""<<fileFieldName<<"\"; filename=\""<<fileName<<"\"\r\n";
    fileHeader<<"Content-Type: "<<fileContentType<<"\r\n\r\n";
    const std::string fileHeaderStr=fileHeader.str();
    const std::string fileFooterStr="\r\n--"+boundary+"--\r\n";
    contentLength+=fileHeaderStr.size();
    contentLength+=fileSize;
    contentLength+=fileFooterStr.size();

    const int fd=openTcpSocket(parsed.host,parsed.port);
    HttpTls tls;
    SSL* ssl=nullptr;
    try{
        if(parsed.scheme=="https"){tls=connectTls(fd,parsed,certPemPath,keyPemPath,allowInsecureTls);ssl=tls.ssl;}

        std::ostringstream req;
        req<<"POST "<<parsed.target<<" HTTP/1.1\r\n";
        req<<"Host: "<<parsed.host<<":"<<parsed.port<<"\r\n";
        req<<"Content-Type: multipart/form-data; boundary="<<boundary<<"\r\n";
        req<<"Accept: application/json\r\n";
        req<<"Connection: close\r\n";
        req<<"Content-Length: "<<contentLength<<"\r\n\r\n";
        const std::string headers=req.str();
        if(ssl) writeAllSsl(ssl,headers.data(),headers.size()); else writeAllFd(fd,headers.data(),headers.size());
        for(const std::string& part:fieldParts){
            if(ssl) writeAllSsl(ssl,part.data(),part.size()); else writeAllFd(fd,part.data(),part.size());
        }
        if(ssl) writeAllSsl(ssl,fileHeaderStr.data(),fileHeaderStr.size()); else writeAllFd(fd,fileHeaderStr.data(),fileHeaderStr.size());

        std::ifstream in(filePath,std::ios::binary);
        if(!in) throw std::runtime_error("Falha abrindo arquivo para upload: "+filePath.string());
        std::array<char,64*1024> buf{};
        while(in){
            in.read(buf.data(),(std::streamsize)buf.size());
            const std::streamsize got=in.gcount();
            if(got<=0) break;
            if(ssl) writeAllSsl(ssl,buf.data(),(std::size_t)got); else writeAllFd(fd,buf.data(),(std::size_t)got);
        }
        if(!in.eof()&&in.fail()) throw std::runtime_error("Falha lendo arquivo durante upload.");

        if(ssl) writeAllSsl(ssl,fileFooterStr.data(),fileFooterStr.size()); else writeAllFd(fd,fileFooterStr.data(),fileFooterStr.size());
        std::string raw=readHttpResponseBody(fd,ssl);
        ::close(fd);
        const std::size_t headerEnd=raw.find("\r\n\r\n");
        if(headerEnd==std::string::npos) throw std::runtime_error("Resposta HTTP invalida no upload.");
        std::istringstream hs(raw.substr(0,headerEnd));
        std::string statusLine;std::getline(hs,statusLine);
        std::istringstream sl(statusLine);
        std::string httpVersion;
        HttpResponse resp;
        sl>>httpVersion>>resp.status;
        resp.body=raw.substr(headerEnd+4);
        return resp;
    }catch(...){
        ::close(fd);
        throw;
    }
}

std::map<std::string,std::string> loadIdentityMeta(const keeply::fs::path& metaPath){
    std::map<std::string,std::string> out;
    std::ifstream in(metaPath);
    if(!in) return out;
    std::string line;
    while(std::getline(in,line)){
        const std::size_t eq=line.find('=');
        if(eq==std::string::npos) continue;
        out[line.substr(0,eq)]=line.substr(eq+1);
    }
    return out;
}

void saveIdentityMeta(const keeply::AgentIdentity& identity){
    std::ofstream out(identity.metaPath,std::ios::trunc);
    if(!out) throw std::runtime_error("Falha ao persistir metadata da identidade do agente.");
    out<<"device_id="<<identity.deviceId<<"\n";
    out<<"user_id="<<identity.userId<<"\n";
    out<<"fingerprint_sha256="<<identity.fingerprintSha256<<"\n";
    out<<"pairing_code="<<identity.pairingCode<<"\n";
    out<<"cert_pem="<<identity.certPemPath.string()<<"\n";
    out<<"key_pem="<<identity.keyPemPath.string()<<"\n";
}

std::string fingerprintFromX509(X509* cert){
    int derLen=i2d_X509(cert,nullptr);
    if(derLen<=0) throw std::runtime_error("Falha ao serializar certificado DER.");
    std::vector<unsigned char> der((std::size_t)derLen);
    unsigned char* ptr=der.data();
    if(i2d_X509(cert,&ptr)!=derLen) throw std::runtime_error("Falha ao serializar certificado DER.");
    unsigned char digest[SHA256_DIGEST_LENGTH];
    SHA256(der.data(),der.size(),digest);
    return hexEncode(digest,sizeof(digest));
}

std::string fingerprintFromPemFile(const keeply::fs::path& certPemPath){
    FILE* fp=std::fopen(certPemPath.c_str(),"rb");
    if(!fp) throw std::runtime_error("Falha ao abrir certificado do agente.");
    X509* cert=PEM_read_X509(fp,nullptr,nullptr,nullptr);
    std::fclose(fp);
    if(!cert) throw std::runtime_error("Falha ao ler certificado PEM do agente.");
    const std::string fpHex=fingerprintFromX509(cert);
    X509_free(cert);
    return fpHex;
}

keeply::AgentIdentity generateSelfSignedIdentity(const keeply::WsClientConfig& config){
    keeply::AgentIdentity identity;
    identity.certPemPath=config.identityDir/"agent-cert.pem";
    identity.keyPemPath=config.identityDir/"agent-key.pem";
    identity.metaPath=config.identityDir/"identity.meta";
    EVP_PKEY_CTX* pkeyCtx=EVP_PKEY_CTX_new_id(EVP_PKEY_RSA,nullptr);
    if(!pkeyCtx) throw std::runtime_error("Falha ao criar contexto da chave.");
    EVP_PKEY* pkey=nullptr;
    X509* cert=nullptr;
    try{
        if(EVP_PKEY_keygen_init(pkeyCtx)<=0) throw std::runtime_error("Falha ao inicializar keygen.");
        if(EVP_PKEY_CTX_set_rsa_keygen_bits(pkeyCtx,2048)<=0) throw std::runtime_error("Falha ao configurar bits da chave.");
        if(EVP_PKEY_keygen(pkeyCtx,&pkey)<=0) throw std::runtime_error("Falha ao gerar chave privada.");
        EVP_PKEY_CTX_free(pkeyCtx);pkeyCtx=nullptr;
        cert=X509_new();
        if(!cert) throw std::runtime_error("Falha ao criar certificado X509.");
        X509_set_version(cert,2);
        ASN1_INTEGER_set(X509_get_serialNumber(cert),(long)std::time(nullptr));
        X509_gmtime_adj(X509_get_notBefore(cert),0);
        X509_gmtime_adj(X509_get_notAfter(cert),60L*60L*24L*365L*5L);
        X509_set_pubkey(cert,pkey);
        X509_NAME* name=X509_get_subject_name(cert);
        X509_NAME_add_entry_by_txt(name,"CN",MBSTRING_ASC,(const unsigned char*)config.deviceName.c_str(),-1,-1,0);
        X509_set_issuer_name(cert,name);
        if(X509_sign(cert,pkey,EVP_sha256())<=0) throw std::runtime_error("Falha ao assinar certificado do agente.");
        FILE* keyFp=std::fopen(identity.keyPemPath.c_str(),"wb");
        if(!keyFp) throw std::runtime_error("Falha ao criar chave PEM do agente.");
        if(PEM_write_PrivateKey(keyFp,pkey,nullptr,nullptr,0,nullptr,nullptr)!=1){std::fclose(keyFp);throw std::runtime_error("Falha ao salvar chave PEM do agente.");}
        std::fclose(keyFp);
        FILE* certFp=std::fopen(identity.certPemPath.c_str(),"wb");
        if(!certFp) throw std::runtime_error("Falha ao criar cert PEM do agente.");
        if(PEM_write_X509(certFp,cert)!=1){std::fclose(certFp);throw std::runtime_error("Falha ao salvar cert PEM do agente.");}
        std::fclose(certFp);
        identity.fingerprintSha256=fingerprintFromX509(cert);
        X509_free(cert);
        EVP_PKEY_free(pkey);
        return identity;
    }catch(...){
        if(cert) X509_free(cert);
        if(pkey) EVP_PKEY_free(pkey);
        if(pkeyCtx) EVP_PKEY_CTX_free(pkeyCtx);
        throw;
    }
}

std::string extractJsonStringField(const std::string& json,const std::string& field){
    const std::string key="\""+field+"\"";
    const std::size_t keyPos=json.find(key);
    if(keyPos==std::string::npos) return "";
    const std::size_t colon=json.find(':',keyPos+key.size());
    if(colon==std::string::npos) return "";
    const std::size_t q1=json.find('\"',colon+1);
    if(q1==std::string::npos) return "";
    const std::size_t q2=json.find('\"',q1+1);
    if(q2==std::string::npos) return "";
    return json.substr(q1+1,q2-q1-1);
}

std::vector<std::string> splitPipe(const std::string& value){
    std::vector<std::string> out;
    std::size_t start=0;
    while(start<=value.size()){
        const std::size_t pos=value.find('|',start);
        out.push_back(value.substr(start,pos==std::string::npos?std::string::npos:pos-start));
        if(pos==std::string::npos) break;
        start=pos+1;
    }
    return out;
}

BackupStoragePolicy parseBackupStoragePolicy(const std::string& raw){
    BackupStoragePolicy policy;
    const std::string normalized=toLower(keeply::trim(raw));
    if(normalized=="cloud_only"){
        policy.mode="cloud_only";
        policy.keepLocal=false;
        policy.uploadCloud=true;
        policy.deleteLocalAfterUpload=true;
        return policy;
    }
    if(normalized=="local_then_cloud"){
        policy.mode="local_then_cloud";
        policy.keepLocal=true;
        policy.uploadCloud=true;
        policy.deleteLocalAfterUpload=false;
        return policy;
    }
    policy.mode="local_only";
    policy.keepLocal=true;
    policy.uploadCloud=false;
    policy.deleteLocalAfterUpload=false;
    return policy;
}

void deleteLocalArchiveArtifacts(const keeply::fs::path& archivePath){
    std::error_code ec;
    keeply::fs::remove(archivePath,ec);
    ec.clear();
    keeply::fs::path packPath=archivePath;
    packPath.replace_extension(".klyp");
    keeply::fs::remove(packPath,ec);
    ec.clear();
    keeply::fs::remove(keeply::fs::path(packPath.string()+".idx"),ec);
}

struct FsListRow{
    bool isDir=false;
    std::string name;
    std::string fullPath;
    std::uintmax_t size=0;
};

std::string httpUrlFromWsUrl(const std::string& wsUrl,const std::string& path){
    ParsedUrl parsed=parseUrlCommon(wsUrl);
    if(parsed.scheme=="ws") parsed.scheme="http";
    else if(parsed.scheme=="wss") parsed.scheme="https";
    else throw std::runtime_error("URL websocket invalida para derivar URL HTTP.");
    parsed.target=path;
    std::ostringstream oss;
    oss<<parsed.scheme<<"://"<<parsed.host<<":"<<parsed.port<<parsed.target;
    return oss.str();
}

struct PairingStatusResponse{std::string status;std::string deviceId;std::string code;std::string userId;};

PairingStatusResponse parsePairingStatusResponse(const HttpResponse& response){
    PairingStatusResponse out;
    out.status=keeply::trim(extractJsonStringField(response.body,"status"));
    out.deviceId=keeply::trim(extractJsonStringField(response.body,"deviceId"));
    out.code=keeply::trim(extractJsonStringField(response.body,"code"));
    out.userId=keeply::trim(extractJsonStringField(response.body,"userId"));
    return out;
}

PairingStatusResponse startPairing(const keeply::WsClientConfig& config,const keeply::AgentIdentity& identity,const std::string& code){
    const std::string url=httpUrlFromWsUrl(config.url,"/api/devices/pairing/start");
    const std::string body=std::string("{\"code\":\"")+escapeJson(code)+"\",\"deviceName\":\""+escapeJson(config.deviceName)+"\",\"hostName\":\""+escapeJson(config.hostName)+"\",\"os\":\""+escapeJson(config.osName)+"\",\"certFingerprintSha256\":\""+escapeJson(identity.fingerprintSha256)+"\"}";
    const HttpResponse response=httpPostJson(url,body,std::nullopt,std::nullopt,config.allowInsecureTls);
    if(response.status==409) return PairingStatusResponse{"code_conflict","",""};
    if(response.status<200||response.status>=300) throw std::runtime_error("Falha ao iniciar pareamento do agente. HTTP "+std::to_string(response.status)+" | body="+response.body);
    return parsePairingStatusResponse(response);
}

PairingStatusResponse pollPairingStatus(const keeply::WsClientConfig& config,const keeply::AgentIdentity& identity,const std::string& code){
    const std::string url=httpUrlFromWsUrl(config.url,"/api/devices/pairing/status");
    const std::string body=std::string("{\"code\":\"")+escapeJson(code)+"\",\"certFingerprintSha256\":\""+escapeJson(identity.fingerprintSha256)+"\"}";
    const HttpResponse response=httpPostJson(url,body,std::nullopt,std::nullopt,config.allowInsecureTls);
    if(response.status<200||response.status>=300) throw std::runtime_error("Falha ao consultar status do pareamento. HTTP "+std::to_string(response.status)+" | body="+response.body);
    return parsePairingStatusResponse(response);
}

UploadBundleResult uploadArchiveBackup(const keeply::WsClientConfig& config,
                                       const keeply::AgentIdentity& identity,
                                       const keeply::AppState& state,
                                       const std::string& label,
                                       const std::function<void(const UploadProgressSnapshot&)>& onProgress={}){
    if(identity.userId.empty()) throw std::runtime_error("Agente sem userId persistido. Refaça o pareamento.");
    const keeply::fs::path archivePath=keeply::fs::path(state.archive);
    if(!keeply::fs::exists(archivePath)) throw std::runtime_error("Arquivo de backup local nao encontrado para upload.");

    keeply::StorageArchive archive(archivePath);
    const auto bundle=archive.exportCloudBundle("/tmp/keeply/cloud_bundle_export",16ull*1024ull*1024ull);
    const std::string url=httpUrlFromWsUrl(config.url,"/api/agent/backups/upload");
    UploadBundleResult result;
    result.bundleId=bundle.bundleId;
    const std::size_t filesTotal=bundle.files.size();
    std::atomic<std::size_t> uploadedCount{0};
    std::atomic<std::size_t> uploadedBlobCount{0};
    try{
        auto uploadOne=[&](const keeply::StorageArchive::CloudBundleFile& item,std::size_t attempt){
            const std::string role=item.manifest?"manifest":(item.blobPart?"blob":"metadata");
            std::size_t blobPartIndex=0;
            if(item.blobPart){
                const std::size_t marker=item.uploadName.find_last_of('-');
                const std::size_t dot=item.uploadName.rfind('.');
                if(marker!=std::string::npos&&dot!=std::string::npos&&dot>marker+1){
                    blobPartIndex=(std::size_t)std::strtoull(item.uploadName.substr(marker+1,dot-marker-1).c_str(),nullptr,10);
                }
            }
            if(onProgress){
                onProgress(UploadProgressSnapshot{
                    uploadedCount.load(),
                    filesTotal,
                    blobPartIndex,
                    bundle.blobPartCount,
                    item.uploadName,
                    role
                });
            }
            std::vector<MultipartField> fields;
            fields.push_back({"userId",identity.userId});
            fields.push_back({"agentId",identity.deviceId});
            fields.push_back({"folderName",label});
            fields.push_back({"mode","manual"});
            fields.push_back({"sourcePath",state.source});
            fields.push_back({"bundleId",bundle.bundleId});
            fields.push_back({"bundleFileName",item.uploadName});
            fields.push_back({"objectKey",item.objectKey});
            fields.push_back({"bundleRole",role});
            const HttpResponse response=httpPostMultipartFile(
                url,
                fields,
                "file",
                item.path,
                item.uploadName,
                item.contentType,
                std::nullopt,
                std::nullopt,
                config.allowInsecureTls
            );
            if(response.status<200||response.status>=300){
                throw std::runtime_error(
                    "HTTP "+std::to_string(response.status)+
                    " | tentativa="+std::to_string(attempt)+
                    " | body="+response.body
                );
            }
            const std::size_t completed=uploadedCount.fetch_add(1)+1;
            if(item.blobPart) uploadedBlobCount.fetch_add(1);
            if(item.manifest) result.manifestResponse=response;
            if(onProgress){
                onProgress(UploadProgressSnapshot{
                    completed,
                    filesTotal,
                    blobPartIndex,
                    bundle.blobPartCount,
                    item.uploadName,
                    role
                });
            }
        };

        const keeply::StorageArchive::CloudBundleFile* manifestFile=nullptr;
        for(const auto& item:bundle.files){
            if(item.manifest){manifestFile=&item; continue;}
            if(item.blobPart) continue;
            uploadOne(item,1);
            if(!item.path.empty()){
                std::error_code cleanupEc;
                keeply::fs::remove(item.path,cleanupEc);
            }
        }

        keeply::runParallelUploadQueue(
            bundle.blobPartCount,
            keeply::ParallelUploadOptions{2,3,std::chrono::milliseconds(750)},
            [&](std::size_t partIndex,std::size_t attempt){
                auto blobFile=archive.materializeCloudBundleBlob(bundle,partIndex);
                try{
                    uploadOne(blobFile,attempt);
                }catch(...){
                    std::error_code cleanupEc;
                    if(!blobFile.path.empty()) keeply::fs::remove(blobFile.path,cleanupEc);
                    throw;
                }
                std::error_code cleanupEc;
                if(!blobFile.path.empty()) keeply::fs::remove(blobFile.path,cleanupEc);
            }
        );

        if(manifestFile){
            uploadOne(*manifestFile,1);
        }
    }catch(...){
        std::error_code cleanupEc;
        keeply::fs::remove_all(bundle.rootDir,cleanupEc);
        throw;
    }
    std::error_code cleanupEc;
    keeply::fs::remove_all(bundle.rootDir,cleanupEc);
    result.filesUploaded=uploadedCount.load();
    result.blobPartCount=uploadedBlobCount.load();
    if(result.manifestResponse.status==0) throw std::runtime_error("Manifest do bundle cloud nao foi enviado.");
    return result;
}

} // namespace

namespace keeply {

struct KeeplyAgentWsClient::TlsState{
    SSL_CTX* ctx=nullptr;
    SSL* ssl=nullptr;
    ~TlsState(){if(ssl){SSL_shutdown(ssl);SSL_free(ssl);}if(ctx) SSL_CTX_free(ctx);}
};

void KeeplyWsHub::addClient(IWsClientSession* client){
    if(!client) return;
    std::lock_guard<std::mutex> lock(mu_);
    clients_[client->id()]=client;
}

void KeeplyWsHub::removeClient(IWsClientSession* client){
    if(!client) return;
    std::lock_guard<std::mutex> lock(mu_);
    clients_.erase(client->id());
    clientJobSubscription_.erase(client->id());
}

void KeeplyWsHub::broadcastJson(const std::string& json){
    std::vector<IWsClientSession*> snapshot;
    {std::lock_guard<std::mutex> lock(mu_);snapshot.reserve(clients_.size());for(auto& kv:clients_) snapshot.push_back(kv.second);}
    for(auto* c:snapshot) sendSafe_(c,json);
}

void KeeplyWsHub::publishJobEvent(const std::string& jobId,const std::string& json){
    std::vector<IWsClientSession*> targets;
    {std::lock_guard<std::mutex> lock(mu_);
        for(auto& [cid,c]:clients_){
            auto it=clientJobSubscription_.find(cid);
            if(it!=clientJobSubscription_.end()&&it->second==jobId) targets.push_back(c);
        }
    }
    for(auto* c:targets) sendSafe_(c,json);
}

void KeeplyWsHub::onClientMessage(IWsClientSession* client,const std::string& payload){
    if(!client) return;
    if(payload=="ping"){sendSafe_(client,R"({"type":"pong"})");return;}
    const std::string prefix="sub:";
    if(payload.rfind(prefix,0)==0){
        const std::string jobId=payload.substr(prefix.size());
        {std::lock_guard<std::mutex> lock(mu_);clientJobSubscription_[client->id()]=jobId;}
        sendSafe_(client,std::string("{\"type\":\"subscribed\",\"jobId\":\"")+jobId+"\"}");
        return;
    }
    sendSafe_(client,R"({"type":"error","message":"Mensagem WS nao suportada no MVP"})");
}

void KeeplyWsHub::sendSafe_(IWsClientSession* client,const std::string& payload){
    if(!client) return;
    try{client->sendText(payload);}
    catch(const std::exception& e){std::cerr<<"[ws] sendText falhou para client "<<client->id()<<": "<<e.what()<<"\n";}
    catch(...){std::cerr<<"[ws] sendText falhou para client "<<client->id()<<": erro desconhecido\n";}
}

AgentIdentity KeeplyAgentBootstrap::ensureRegistered(const WsClientConfig& config){
    if(trim(config.url).empty()) throw std::runtime_error("URL do backend websocket nao pode ser vazia.");
    fs::create_directories(config.identityDir);
    AgentIdentity identity;
    identity.metaPath=config.identityDir/"identity.meta";
    const auto meta=loadIdentityMeta(identity.metaPath);
    auto itDeviceId=meta.find("device_id");if(itDeviceId!=meta.end()) identity.deviceId=trim(itDeviceId->second);
    auto itUserId=meta.find("user_id");if(itUserId!=meta.end()) identity.userId=trim(itUserId->second);
    auto itFp=meta.find("fingerprint_sha256");if(itFp!=meta.end()) identity.fingerprintSha256=trim(itFp->second);
    auto itPairingCode=meta.find("pairing_code");if(itPairingCode!=meta.end()) identity.pairingCode=trim(itPairingCode->second);
    auto itCert=meta.find("cert_pem");
    auto itKey=meta.find("key_pem");
    identity.certPemPath=(itCert!=meta.end()&&!trim(itCert->second).empty())?fs::path(trim(itCert->second)):(config.identityDir/"agent-cert.pem");
    identity.keyPemPath=(itKey!=meta.end()&&!trim(itKey->second).empty())?fs::path(trim(itKey->second)):(config.identityDir/"agent-key.pem");
    if(!fs::exists(identity.certPemPath)||!fs::exists(identity.keyPemPath)){
        identity=generateSelfSignedIdentity(config);
    }else if(identity.fingerprintSha256.empty()){
        identity.fingerprintSha256=fingerprintFromPemFile(identity.certPemPath);
    }
    for(;;){
        if(identity.pairingCode.empty()) identity.pairingCode=trim(config.pairingCode);
        if(identity.pairingCode.empty()) identity.pairingCode=randomDigits(8);
        PairingStatusResponse started=startPairing(config,identity,identity.pairingCode);
        if(started.status=="code_conflict"){identity.pairingCode.clear();continue;}
        if(!started.code.empty()) identity.pairingCode=started.code;
        if(started.status=="active"){identity.deviceId=started.deviceId;identity.userId=started.userId;identity.pairingCode.clear();break;}
        saveIdentityMeta(identity);
        std::cout<<"Codigo de ativacao Keeply: "<<identity.pairingCode<<" | dispositivo: "<<config.deviceName<<" | host: "<<config.hostName<<"\n";
        for(;;){
            std::this_thread::sleep_for(std::chrono::milliseconds(std::max(1000,config.pairingPollIntervalMs)));
            PairingStatusResponse status=pollPairingStatus(config,identity,identity.pairingCode);
            if(status.status=="active"){identity.deviceId=status.deviceId;identity.userId=status.userId;identity.pairingCode.clear();break;}
            if(status.status=="expired"||status.status=="missing"){identity.pairingCode.clear();break;}
        }
        if(!identity.deviceId.empty()) break;
    }
    saveIdentityMeta(identity);
    return identity;
}

KeeplyAgentWsClient::KeeplyAgentWsClient(std::shared_ptr<KeeplyApi> api,AgentIdentity identity):api_(std::move(api)),identity_(std::move(identity)){
    if(!api_) throw std::runtime_error("KeeplyAgentWsClient requer KeeplyApi.");
}

KeeplyAgentWsClient::~KeeplyAgentWsClient(){try{close();}catch(...){}} 

void KeeplyAgentWsClient::connect(const WsClientConfig& config){
    close();
    config_=config;
    if(trim(config_.agentId).empty()) throw std::runtime_error("deviceId do websocket nao pode ser vazio.");
    const UrlParts url=parseUrl_(config_.url);
    connectSocket_(url);
    try{
        performHandshake_(url);
        {std::lock_guard<std::mutex> lock(mu_);connected_=true;closeSent_=false;}
        sendHello_();
    }catch(...){close();throw;}
}

void KeeplyAgentWsClient::run(){
    for(;;){
        unsigned char opcode=0;
        std::string payload;
        if(!readFrame_(opcode,payload)) break;
        if(opcode==0x1){handleServerMessage_(payload);continue;}
        if(opcode==0x8){try{sendClose_(1000,"");}catch(...){}
            break;
        }
        if(opcode==0x9){sendPong_(payload);continue;}
        if(opcode==0xA) continue;
        throw std::runtime_error("Opcode websocket nao suportado recebido do servidor.");
    }
    close();
}

void KeeplyAgentWsClient::close(){
    bool shouldSendClose=false;
    {std::lock_guard<std::mutex> lock(mu_);shouldSendClose=connected_&&!closeSent_&&sockfd_>=0;}
    if(shouldSendClose){try{sendClose_(1000,"client.shutdown");}catch(...){}} 
    int fd=-1;
    {std::lock_guard<std::mutex> lock(mu_);fd=sockfd_;sockfd_=-1;connected_=false;closeSent_=true;recvBuffer_.clear();tls_.reset();}
    if(fd>=0){::shutdown(fd,SHUT_RDWR);::close(fd);}
}

bool KeeplyAgentWsClient::isConnected() const{std::lock_guard<std::mutex> lock(mu_);return connected_;}

void KeeplyAgentWsClient::sendText(const std::string& payload){sendFrame_(0x1,payload);}

KeeplyAgentWsClient::UrlParts KeeplyAgentWsClient::parseUrl_(const std::string& url){
    const ParsedUrl parsed=parseUrlCommon(url);
    if(parsed.scheme!="ws"&&parsed.scheme!="wss") throw std::runtime_error("Somente URLs ws:// ou wss:// sao suportadas.");
    UrlParts out;out.scheme=parsed.scheme;out.host=parsed.host;out.port=parsed.port;out.target=parsed.target;
    return out;
}

std::string KeeplyAgentWsClient::escapeJson_(const std::string& value){return escapeJson(value);}
std::string KeeplyAgentWsClient::urlEncode_(const std::string& value){return urlEncode(value);}
std::string KeeplyAgentWsClient::httpUrlFromWsUrl_(const std::string& wsUrl,const std::string& path){return httpUrlFromWsUrl(wsUrl,path);}

void KeeplyAgentWsClient::connectSocket_(const UrlParts& url){
    const int fd=openTcpSocket(url.host,url.port);
    {std::lock_guard<std::mutex> lock(mu_);sockfd_=fd;recvBuffer_.clear();}
    if(url.scheme=="wss") configureTls_(url);
}

void KeeplyAgentWsClient::configureTls_(const UrlParts& url){
    auto tls=std::make_unique<TlsState>();
    tls->ctx=SSL_CTX_new(TLS_client_method());
    if(!tls->ctx) throw std::runtime_error("Falha ao criar SSL_CTX do websocket.");
    if(config_.allowInsecureTls){
        SSL_CTX_set_verify(tls->ctx,SSL_VERIFY_NONE,nullptr);
    }else{
        SSL_CTX_set_verify(tls->ctx,SSL_VERIFY_PEER,nullptr);
        if(SSL_CTX_set_default_verify_paths(tls->ctx)!=1) throw std::runtime_error("Falha ao configurar trust store do websocket.");
    }
    if(!identity_.certPemPath.empty()&&!identity_.keyPemPath.empty()){
        if(SSL_CTX_use_certificate_file(tls->ctx,identity_.certPemPath.c_str(),SSL_FILETYPE_PEM)!=1) throw std::runtime_error("Falha ao carregar certificado cliente para websocket.");
        if(SSL_CTX_use_PrivateKey_file(tls->ctx,identity_.keyPemPath.c_str(),SSL_FILETYPE_PEM)!=1) throw std::runtime_error("Falha ao carregar chave privada cliente para websocket.");
    }
    tls->ssl=SSL_new(tls->ctx);
    if(!tls->ssl) throw std::runtime_error("Falha ao criar SSL do websocket.");
    SSL_set_fd(tls->ssl,sockfd_);
    SSL_set_tlsext_host_name(tls->ssl,url.host.c_str());
    if(SSL_connect(tls->ssl)!=1) throw std::runtime_error("Falha no handshake TLS do websocket.");
    std::lock_guard<std::mutex> lock(mu_);
    tls_=std::move(tls);
}

void KeeplyAgentWsClient::performHandshake_(const UrlParts& url){
    const std::string key=randomBase64(16);
    std::string target=url.target;
    const std::string separator=target.find('?')==std::string::npos?"?":"&";
    target+=separator+std::string("deviceId=")+urlEncode_(config_.agentId);
    std::ostringstream req;
    req<<"GET "<<target<<" HTTP/1.1\r\n";
    req<<"Host: "<<url.host<<":"<<url.port<<"\r\n";
    req<<"Upgrade: websocket\r\n";
    req<<"Connection: Upgrade\r\n";
    req<<"Sec-WebSocket-Key: "<<key<<"\r\n";
    req<<"Sec-WebSocket-Version: 13\r\n";
    req<<"User-Agent: keeply-agent/1.0\r\n";
    req<<"\r\n";
    const std::string request=req.str();
    writeRaw_(request.data(),request.size());
    const std::string header=readHttpHeader_();
    std::istringstream iss(header);
    std::string statusLine;
    std::getline(iss,statusLine);
    if(statusLine.find("101")==std::string::npos) throw std::runtime_error("Handshake websocket rejeitado pelo backend: "+trim(statusLine));
    unsigned char sha[SHA_DIGEST_LENGTH];
    const std::string acceptInput=key+"258EAFA5-E914-47DA-95CA-C5AB0DC85B11";
    SHA1((const unsigned char*)acceptInput.data(),acceptInput.size(),sha);
    const std::string expectedAccept=base64Encode(sha,sizeof(sha));
    bool hasUpgrade=false,hasConnection=false,hasAccept=false;
    std::string line;
    while(std::getline(iss,line)){
        if(!line.empty()&&line.back()=='\r') line.pop_back();
        const std::size_t colon=line.find(':');
        if(colon==std::string::npos) continue;
        const std::string keyName=toLower(trim(line.substr(0,colon)));
        const std::string value=trim(line.substr(colon+1));
        const std::string lowerValue=toLower(value);
        if(keyName=="upgrade"&&lowerValue=="websocket") hasUpgrade=true;
        if(keyName=="connection"&&lowerValue.find("upgrade")!=std::string::npos) hasConnection=true;
        if(keyName=="sec-websocket-accept"&&value==expectedAccept) hasAccept=true;
    }
    if(!hasUpgrade||!hasConnection||!hasAccept) throw std::runtime_error("Handshake websocket invalido recebido do backend.");
}

void KeeplyAgentWsClient::handleServerMessage_(const std::string& payload){
    if(payload.empty()) return;
    if(!payload.empty()&&payload.front()=='{'){
        const std::string type=keeply::trim(extractJsonStringField(payload,"type"));
        if(!type.empty()) std::cout<<"[ws] << type="<<type<<" | payload="<<payload<<"\n";
        else std::cout<<"[ws] << payload="<<payload<<"\n";
        return;
    }
    try{
        if(payload=="ping"){sendJson_(R"({"type":"pong"})");return;}
        if(payload=="state"){sendJson_(buildStateJson_());return;}
        if(payload=="snapshots"){sendJson_(buildSnapshotsJson_());return;}
        if(payload=="fs.list"){sendJson_(buildFsListJson_("",""));return;}
        if(payload.rfind("fs.list:",0)==0){
            const std::string args=payload.substr(std::string("fs.list:").size());
            const std::size_t sep=args.find('|');
            std::string requestId;
            std::string targetPath;
            if(sep==std::string::npos){
                targetPath=args;
            }else{
                requestId=args.substr(0,sep);
                targetPath=args.substr(sep+1);
            }
            sendJson_(buildFsListJson_(requestId,targetPath));
            return;
        }
        if(payload.rfind("scan.scope:",0)==0){
            const std::string scopeId=payload.substr(std::string("scan.scope:").size());
            api_->setScanScope(scopeId);
            std::ostringstream oss;
            oss<<"{"<<"\"type\":\"scan.scope.updated\","<<"\"agentId\":\""<<escapeJson_(config_.agentId)<<"\","<<"\"source\":\""<<escapeJson_(api_->state().source)<<"\","<<"\"scanScope\":"<<buildScanScopeJson_()<<"}";
            sendJson_(oss.str());
            return;
        }
        if(payload.rfind("config.source:",0)==0){
            api_->setSource(payload.substr(std::string("config.source:").size()));
            std::ostringstream oss;
            oss<<"{"<<"\"type\":\"config.updated\","<<"\"field\":\"source\","<<"\"agentId\":\""<<escapeJson_(config_.agentId)<<"\","<<"\"source\":\""<<escapeJson_(api_->state().source)<<"\","<<"\"scanScope\":"<<buildScanScopeJson_()<<"}";
            sendJson_(oss.str());
            return;
        }
        if(payload.rfind("config.archive:",0)==0){api_->setArchive(payload.substr(std::string("config.archive:").size()));sendJson_(R"({"type":"config.updated","field":"archive"})");return;}
        if(payload.rfind("config.restoreRoot:",0)==0){api_->setRestoreRoot(payload.substr(std::string("config.restoreRoot:").size()));sendJson_(R"({"type":"config.updated","field":"restoreRoot"})");return;}
        if(payload.rfind("backup",0)==0){
            std::string label;
            BackupStoragePolicy storagePolicy;
            BackupProgress latestProgress;
            if(payload.rfind("backup:",0)==0){
                const auto parts=splitPipe(payload.substr(std::string("backup:").size()));
                if(!parts.empty()) label=parts[0];
                for(std::size_t i=1;i<parts.size();++i){
                    const std::string part=parts[i];
                    if(part.rfind("storage=",0)==0){
                        storagePolicy=parseBackupStoragePolicy(part.substr(std::string("storage=").size()));
                    }
                }
            }
            std::cout<<"[backup] iniciado";
            if(!label.empty()) std::cout<<" | label="<<label;
            std::cout<<" | source="<<api_->state().source<<"\n";
            sendJson_(std::string("{\"type\":\"backup.started\",\"label\":\"")+escapeJson_(label)+"\",\"source\":\""+escapeJson_(api_->state().source)+"\",\"scanScope\":"+buildScanScopeJson_()+"}");
            try{
                const BackupStats stats=api_->runBackup(label,[this,label,&latestProgress](const BackupProgress& progress){
                    latestProgress=progress;
                    std::ostringstream progressJson;
                    progressJson
                        <<"{"<<"\"type\":\"backup.progress\","
                        <<"\"label\":\""<<escapeJson_(label)<<"\","
                        <<"\"source\":\""<<escapeJson_(api_->state().source)<<"\","
                        <<"\"scanScope\":"<<buildScanScopeJson_()<<","
                        <<"\"phase\":\""<<escapeJson_(progress.phase)<<"\","
                        <<"\"discoveryComplete\":"<<(progress.discoveryComplete?"true":"false")<<","
                        <<"\"currentFile\":\""<<escapeJson_(progress.currentFile)<<"\","
                        <<"\"filesQueued\":"<<progress.filesQueued<<","
                        <<"\"filesCompleted\":"<<progress.filesCompleted<<","
                        <<"\"filesScanned\":"<<progress.stats.scanned<<","
                        <<"\"filesAdded\":"<<progress.stats.added<<","
                        <<"\"filesUnchanged\":"<<progress.stats.reused<<","
                        <<"\"chunksNew\":"<<progress.stats.uniqueChunksInserted<<","
                        <<"\"bytesRead\":"<<progress.stats.bytesRead<<","
                        <<"\"warnings\":"<<progress.stats.warnings
                        <<"}";
                    sendJson_(progressJson.str());
                });
                const std::size_t chunksReused=(stats.chunks>=stats.uniqueChunksInserted)?(stats.chunks-stats.uniqueChunksInserted):0;
                std::ostringstream oss;
                oss<<"{"<<"\"type\":\"backup.finished\","<<"\"label\":\""<<escapeJson_(label)<<"\","<<"\"source\":\""<<escapeJson_(api_->state().source)<<"\","<<"\"scanScope\":"<<buildScanScopeJson_()<<","<<"\"filesScanned\":"<<stats.scanned<<","<<"\"filesAdded\":"<<stats.added<<","<<"\"filesUnchanged\":"<<stats.reused<<","<<"\"chunksNew\":"<<stats.uniqueChunksInserted<<","<<"\"chunksReused\":"<<chunksReused<<","<<"\"bytesRead\":"<<stats.bytesRead<<","<<"\"warnings\":"<<stats.warnings<<"}";
                sendJson_(oss.str());
                if(storagePolicy.uploadCloud){
                    try{
                        sendJson_(std::string("{\"type\":\"backup.upload.started\",\"label\":\"")+escapeJson_(label)+"\",\"source\":\""+escapeJson_(api_->state().source)+"\",\"scanScope\":"+buildScanScopeJson_()+",\"storageMode\":\""+escapeJson_(storagePolicy.mode)+"\"}");
                        const UploadBundleResult uploadResp=uploadArchiveBackup(
                            config_,
                            identity_,
                            api_->state(),
                            label,
                            [this,label,&storagePolicy](const UploadProgressSnapshot& progress){
                                std::ostringstream uploadProgressJson;
                                uploadProgressJson
                                    <<"{"<<"\"type\":\"backup.upload.progress\","
                                    <<"\"label\":\""<<escapeJson_(label)<<"\","
                                    <<"\"source\":\""<<escapeJson_(api_->state().source)<<"\","
                                    <<"\"scanScope\":"<<buildScanScopeJson_()<<","
                                    <<"\"storageMode\":\""<<escapeJson_(storagePolicy.mode)<<"\","
                                    <<"\"phase\":\"uploading\","
                                    <<"\"currentObject\":\""<<escapeJson_(progress.currentObject)<<"\","
                                    <<"\"bundleRole\":\""<<escapeJson_(progress.bundleRole)<<"\","
                                    <<"\"filesUploaded\":"<<progress.filesUploaded<<","
                                    <<"\"filesTotal\":"<<progress.filesTotal<<","
                                    <<"\"blobPartIndex\":"<<progress.blobPartIndex<<","
                                    <<"\"blobPartCount\":"<<progress.blobPartCount
                                    <<"}";
                                sendJson_(uploadProgressJson.str());
                            }
                        );
                        const std::string storageId=trim(extractJsonStringField(uploadResp.manifestResponse.body,"storageId"));
                        const std::string storageScope=trim(extractJsonStringField(uploadResp.manifestResponse.body,"storageScope"));
                        const std::string uri=trim(extractJsonStringField(uploadResp.manifestResponse.body,"uri"));
                        const std::string bucket=trim(extractJsonStringField(uploadResp.manifestResponse.body,"bucket"));
                        if(storagePolicy.deleteLocalAfterUpload){
                            deleteLocalArchiveArtifacts(keeply::fs::path(api_->state().archive));
                        }
                        std::ostringstream uploadedJson;
                        uploadedJson
                            <<"{"<<"\"type\":\"backup.uploaded\","
                            <<"\"label\":\""<<escapeJson_(label)<<"\","
                            <<"\"source\":\""<<escapeJson_(api_->state().source)<<"\","
                            <<"\"scanScope\":"<<buildScanScopeJson_()<<","
                            <<"\"storageMode\":\""<<escapeJson_(storagePolicy.mode)<<"\","
                            <<"\"storageId\":\""<<escapeJson_(storageId)<<"\","
                            <<"\"storageScope\":\""<<escapeJson_(storageScope)<<"\","
                            <<"\"bucket\":\""<<escapeJson_(bucket)<<"\","
                            <<"\"uri\":\""<<escapeJson_(uri)<<"\","
                            <<"\"bundleId\":\""<<escapeJson_(uploadResp.bundleId)<<"\","
                            <<"\"filesUploaded\":"<<uploadResp.filesUploaded<<","
                            <<"\"blobPartCount\":"<<uploadResp.blobPartCount
                            <<"}";
                        sendJson_(uploadedJson.str());
                    }catch(const std::exception& uploadEx){
                        std::ostringstream uploadFailedJson;
                        uploadFailedJson
                            <<"{"<<"\"type\":\"backup.upload.failed\","
                            <<"\"label\":\""<<escapeJson_(label)<<"\","
                            <<"\"source\":\""<<escapeJson_(api_->state().source)<<"\","
                            <<"\"scanScope\":"<<buildScanScopeJson_()<<","
                            <<"\"storageMode\":\""<<escapeJson_(storagePolicy.mode)<<"\","
                            <<"\"message\":\""<<escapeJson_(uploadEx.what())<<"\""
                            <<"}";
                        sendJson_(uploadFailedJson.str());
                    }
                }else{
                    std::ostringstream localOnlyJson;
                    localOnlyJson
                        <<"{"<<"\"type\":\"backup.storage.applied\","
                        <<"\"label\":\""<<escapeJson_(label)<<"\","
                        <<"\"source\":\""<<escapeJson_(api_->state().source)<<"\","
                        <<"\"scanScope\":"<<buildScanScopeJson_()<<","
                        <<"\"storageMode\":\""<<escapeJson_(storagePolicy.mode)<<"\","
                        <<"\"message\":\"Backup mantido somente no destino local atribuido.\""
                        <<"}";
                    sendJson_(localOnlyJson.str());
                }
                std::cout
                    <<"[backup] concluido"
                    <<" | scanned="<<stats.scanned
                    <<" added="<<stats.added
                    <<" unchanged="<<stats.reused
                    <<" bytes="<<stats.bytesRead
                    <<" warnings="<<stats.warnings
                    <<"\n";
                std::cout<<"[agent] backup finalizado. Agente segue online aguardando comandos.\n";
            }catch(const std::exception& e){
                std::ostringstream failedJson;
                failedJson
                    <<"{"<<"\"type\":\"backup.failed\","
                    <<"\"label\":\""<<escapeJson_(label)<<"\","
                    <<"\"source\":\""<<escapeJson_(api_->state().source)<<"\","
                    <<"\"scanScope\":"<<buildScanScopeJson_()<<","
                    <<"\"phase\":\"failed\","
                    <<"\"discoveryComplete\":"<<(latestProgress.discoveryComplete?"true":"false")<<","
                    <<"\"currentFile\":\""<<escapeJson_(latestProgress.currentFile)<<"\","
                    <<"\"filesQueued\":"<<latestProgress.filesQueued<<","
                    <<"\"filesCompleted\":"<<latestProgress.filesCompleted<<","
                    <<"\"filesScanned\":"<<latestProgress.stats.scanned<<","
                    <<"\"filesAdded\":"<<latestProgress.stats.added<<","
                    <<"\"filesUnchanged\":"<<latestProgress.stats.reused<<","
                    <<"\"chunksNew\":"<<latestProgress.stats.uniqueChunksInserted<<","
                    <<"\"bytesRead\":"<<latestProgress.stats.bytesRead<<","
                    <<"\"warnings\":"<<latestProgress.stats.warnings<<","
                    <<"\"message\":\""<<escapeJson_(e.what())<<"\""
                    <<"}";
                sendJson_(failedJson.str());
                std::cout<<"[backup] falhou";
                if(!label.empty()) std::cout<<" | label="<<label;
                std::cout<<" | error="<<e.what()<<"\n";
                std::cout<<"[agent] agente segue online aguardando comandos.\n";
                throw;
            }
            return;
        }
        if(payload.rfind("restore.file:",0)==0){
            const auto parts=splitPipe(payload.substr(std::string("restore.file:").size()));
            if(parts.size()<2) throw std::runtime_error("Formato restore.file invalido. Use restore.file:snapshot|relPath|outRoot(opcional)");
            const std::optional<fs::path> outRoot=(parts.size()>=3&&!parts[2].empty())?std::optional<fs::path>(fs::path(parts[2])):std::nullopt;
            api_->restoreFile(parts[0],parts[1],outRoot);
            sendJson_(std::string("{\"type\":\"restore.file.finished\",\"snapshot\":\"")+escapeJson_(parts[0])+"\",\"path\":\""+escapeJson_(parts[1])+"\"}");
            return;
        }
        if(payload.rfind("restore.snapshot:",0)==0){
            const auto parts=splitPipe(payload.substr(std::string("restore.snapshot:").size()));
            if(parts.empty()||parts[0].empty()) throw std::runtime_error("Formato restore.snapshot invalido. Use restore.snapshot:snapshot|outRoot(opcional)");
            const std::optional<fs::path> outRoot=(parts.size()>=2&&!parts[1].empty())?std::optional<fs::path>(fs::path(parts[1])):std::nullopt;
            api_->restoreSnapshot(parts[0],outRoot);
            sendJson_(std::string("{\"type\":\"restore.snapshot.finished\",\"snapshot\":\"")+escapeJson_(parts[0])+"\"}");
            return;
        }
        sendJson_(std::string("{\"type\":\"error\",\"message\":\"Comando websocket nao suportado: ")+escapeJson_(payload)+"\"}");
    }catch(const std::exception& e){
        sendJson_(std::string("{\"type\":\"error\",\"message\":\"")+escapeJson_(e.what())+"\"}");
    }
}

void KeeplyAgentWsClient::sendHello_(){
    const auto& s=api_->state();
    std::ostringstream oss;
    oss<<"{"<<"\"type\":\"agent.hello\","<<"\"deviceId\":\""<<escapeJson_(config_.agentId)<<"\","<<"\"agentId\":\""<<escapeJson_(config_.agentId)<<"\","<<"\"fingerprintSha256\":\""<<escapeJson_(identity_.fingerprintSha256)<<"\","<<"\"connectedAt\":\""<<escapeJson_(nowIsoLocal())<<"\","<<"\"source\":\""<<escapeJson_(s.source)<<"\","<<"\"archive\":\""<<escapeJson_(s.archive)<<"\","<<"\"restoreRoot\":\""<<escapeJson_(s.restoreRoot)<<"\","<<"\"scanScope\":"<<buildScanScopeJson_()<<"}";
    sendJson_(oss.str());
}

void KeeplyAgentWsClient::sendJson_(const std::string& payload){sendText(payload);}
void KeeplyAgentWsClient::sendPong_(const std::string& payload){sendFrame_(0xA,payload);}

void KeeplyAgentWsClient::sendClose_(std::uint16_t code,const std::string& reason){
    std::string payload;
    payload.push_back((char)((code>>8)&0xFF));
    payload.push_back((char)(code&0xFF));
    payload+=reason;
    sendFrame_(0x8,payload);
    std::lock_guard<std::mutex> lock(mu_);
    closeSent_=true;
}

void KeeplyAgentWsClient::sendFrame_(unsigned char opcode,const std::string& payload){
    std::lock_guard<std::mutex> lock(mu_);
    ensureConnected_();
    std::vector<unsigned char> frame;
    frame.reserve(payload.size()+16);
    frame.push_back((unsigned char)(0x80u|(opcode&0x0Fu)));
    const std::size_t len=payload.size();
    if(len<=125){frame.push_back((unsigned char)(0x80u|len));}
    else if(len<=0xFFFFu){
        frame.push_back(0x80u|126u);
        frame.push_back((unsigned char)((len>>8)&0xFFu));
        frame.push_back((unsigned char)(len&0xFFu));
    }else{
        frame.push_back(0x80u|127u);
        for(int shift=56;shift>=0;shift-=8) frame.push_back((unsigned char)(((std::uint64_t)len>>shift)&0xFFu));
    }
    std::array<unsigned char,4> mask{};
    std::random_device rd;
    for(auto& b:mask) b=(unsigned char)rd();
    frame.insert(frame.end(),mask.begin(),mask.end());
    for(std::size_t i=0;i<payload.size();++i) frame.push_back((unsigned char)payload[i]^mask[i%mask.size()]);
    writeRaw_(frame.data(),frame.size());
}

std::string KeeplyAgentWsClient::readHttpHeader_(){
    for(;;){
        const std::size_t pos=recvBuffer_.find("\r\n\r\n");
        if(pos!=std::string::npos){
            const std::size_t end=pos+4;
            const std::string out=recvBuffer_.substr(0,end);
            recvBuffer_.erase(0,end);
            return out;
        }
        char tmp[4096];
        const std::size_t n=readRaw_(tmp,sizeof(tmp));
        if(n==0) throw std::runtime_error("Conexao fechada durante o handshake websocket.");
        recvBuffer_.append(tmp,n);
        if(recvBuffer_.size()>(1u<<20)) throw std::runtime_error("Cabecalho websocket muito grande.");
    }
}

bool KeeplyAgentWsClient::readFrame_(unsigned char& opcode,std::string& payload){
    auto ensureBytes=[&](std::size_t needed){
        while(recvBuffer_.size()<needed){
            char tmp[4096];
            const std::size_t n=readRaw_(tmp,sizeof(tmp));
            if(n==0) return false;
            recvBuffer_.append(tmp,n);
        }
        return true;
    };
    if(!ensureBytes(2)) return false;
    const unsigned char b0=(unsigned char)recvBuffer_[0];
    const unsigned char b1=(unsigned char)recvBuffer_[1];
    const bool fin=(b0&0x80u)!=0;
    opcode=(unsigned char)(b0&0x0Fu);
    const bool masked=(b1&0x80u)!=0;
    std::uint64_t len=(std::uint64_t)(b1&0x7Fu);
    std::size_t offset=2;
    if(len==126){
        if(!ensureBytes(4)) return false;
        len=((std::uint64_t)(unsigned char)recvBuffer_[2]<<8)|(std::uint64_t)(unsigned char)recvBuffer_[3];
        offset=4;
    }else if(len==127){
        if(!ensureBytes(10)) return false;
        len=0;
        for(std::size_t i=0;i<8;++i) len=(len<<8)|(std::uint64_t)(unsigned char)recvBuffer_[2+i];
        offset=10;
    }
    std::array<unsigned char,4> mask{};
    if(masked){
        if(!ensureBytes(offset+4)) return false;
        for(std::size_t i=0;i<mask.size();++i) mask[i]=(unsigned char)recvBuffer_[offset+i];
        offset+=4;
    }
    if(len>(std::uint64_t)std::numeric_limits<std::size_t>::max()) throw std::runtime_error("Frame websocket grande demais.");
    const std::size_t payloadLen=(std::size_t)len;
    if(!ensureBytes(offset+payloadLen)) return false;
    payload.assign(recvBuffer_.data()+(std::ptrdiff_t)offset,payloadLen);
    recvBuffer_.erase(0,offset+payloadLen);
    if(masked) for(std::size_t i=0;i<payload.size();++i) payload[i]=(char)((unsigned char)payload[i]^mask[i%mask.size()]);
    if(!fin) throw std::runtime_error("Frames websocket fragmentados nao sao suportados neste cliente.");
    return true;
}

std::string KeeplyAgentWsClient::buildScanScopeJson_() const{
    const auto& scanScope=api_->state().scanScope;
    std::ostringstream oss;
    oss<<"{"<<"\"id\":\""<<escapeJson_(scanScope.id)<<"\","<<"\"label\":\""<<escapeJson_(scanScope.label)<<"\","<<"\"requestedPath\":\""<<escapeJson_(scanScope.requestedPath)<<"\","<<"\"resolvedPath\":\""<<escapeJson_(scanScope.resolvedPath)<<"\""<<"}";
    return oss.str();
}

std::string KeeplyAgentWsClient::buildStateJson_() const{
    const auto& s=api_->state();
    std::ostringstream oss;
    oss<<"{"<<"\"type\":\"state\","<<"\"deviceId\":\""<<escapeJson_(config_.agentId)<<"\","<<"\"agentId\":\""<<escapeJson_(config_.agentId)<<"\","<<"\"source\":\""<<escapeJson_(s.source)<<"\","<<"\"archive\":\""<<escapeJson_(s.archive)<<"\","<<"\"restoreRoot\":\""<<escapeJson_(s.restoreRoot)<<"\","<<"\"scanScope\":"<<buildScanScopeJson_()<<","<<"\"archiveSplitEnabled\":"<<(s.archiveSplitEnabled?"true":"false")<<","<<"\"archiveSplitMaxBytes\":"<<s.archiveSplitMaxBytes<<"}";
    return oss.str();
}

std::string KeeplyAgentWsClient::buildSnapshotsJson_(){
    const auto rows=api_->listSnapshots();
    std::ostringstream oss;
    oss<<"{"<<"\"type\":\"snapshots\","<<"\"deviceId\":\""<<escapeJson_(config_.agentId)<<"\","<<"\"agentId\":\""<<escapeJson_(config_.agentId)<<"\","<<"\"items\":[";
    for(std::size_t i=0;i<rows.size();++i){
        if(i) oss<<",";
        const auto& r=rows[i];
        oss<<"{"<<"\"id\":"<<r.id<<","<<"\"createdAt\":\""<<escapeJson_(r.createdAt)<<"\","<<"\"sourceRoot\":\""<<escapeJson_(r.sourceRoot)<<"\","<<"\"label\":\""<<escapeJson_(r.label)<<"\","<<"\"fileCount\":"<<r.fileCount<<"}";
    }
    oss<<"]}";
    return oss.str();
}

std::string KeeplyAgentWsClient::buildFsListJson_(const std::string& requestId,const std::string& requestedPath) const{
    const auto& s=api_->state();
    std::string targetRaw=trim(requestedPath);
    if(targetRaw.empty()) targetRaw=trim(s.source);
    fs::path targetPath=targetRaw.empty()?fs::current_path():fs::path(targetRaw);
    std::error_code ec;
    if(!targetPath.is_absolute()) targetPath=fs::absolute(targetPath,ec);
    if(ec){targetPath=fs::current_path();ec.clear();}
    if(!fs::exists(targetPath,ec) || ec){
        std::ostringstream err;
        err<<"{"<<"\"type\":\"fs.list\","<<"\"requestId\":\""<<escapeJson_(trim(requestId))<<"\","<<"\"path\":\""<<escapeJson_(targetPath.string())<<"\","<<"\"error\":\"caminho-nao-encontrado\","<<"\"items\":[]}";
        return err.str();
    }
    if(!fs::is_directory(targetPath,ec) || ec){
        ec.clear();
        const fs::path parent=targetPath.parent_path();
        if(!parent.empty() && fs::is_directory(parent,ec) && !ec){
            targetPath=parent;
        }else{
            ec.clear();
        }
    }

    static constexpr std::size_t kFsListMaxItems=80;
    static constexpr std::size_t kFsListMaxEstimatedJsonBytes=24*1024;
    std::vector<FsListRow> rows;
    rows.reserve(kFsListMaxItems);
    bool truncated=false;
    std::size_t estimatedJsonBytes=512;
    for(fs::directory_iterator it(targetPath,fs::directory_options::skip_permission_denied,ec),end; it!=end; it.increment(ec)){
        if(ec){ec.clear();continue;}
        const fs::path itemPath=it->path();
        const std::string name=itemPath.filename().string();
        if(name.empty()) continue;
        FsListRow row;
        row.name=name;
        row.fullPath=itemPath.string();
        row.isDir=it->is_directory(ec);
        if(ec){row.isDir=false;ec.clear();}
        if(!row.isDir) continue;
        row.size=0;
        const std::size_t rowEstimate=row.name.size()+row.fullPath.size()+96u;
        if(!rows.empty() && estimatedJsonBytes+rowEstimate>kFsListMaxEstimatedJsonBytes){
            truncated=true;
            break;
        }
        rows.push_back(std::move(row));
        estimatedJsonBytes+=rowEstimate;
        if(rows.size()>=kFsListMaxItems){truncated=true;break;}
    }
    std::sort(rows.begin(),rows.end(),[](const FsListRow& a,const FsListRow& b){
        if(a.isDir!=b.isDir) return a.isDir>b.isDir;
        return toLower(a.name)<toLower(b.name);
    });

    std::string parentPath;
    if(targetPath.has_parent_path()) parentPath=targetPath.parent_path().string();
    std::ostringstream oss;
    oss<<"{"
       <<"\"type\":\"fs.list\","
       <<"\"requestId\":\""<<escapeJson_(trim(requestId))<<"\","
       <<"\"path\":\""<<escapeJson_(targetPath.string())<<"\","
       <<"\"parentPath\":\""<<escapeJson_(parentPath)<<"\","
       <<"\"truncated\":"<<(truncated?"true":"false")<<","
       <<"\"items\":[";
    for(std::size_t i=0;i<rows.size();++i){
        if(i) oss<<",";
        const auto& row=rows[i];
        oss<<"{"
           <<"\"name\":\""<<escapeJson_(row.name)<<"\","
           <<"\"path\":\""<<escapeJson_(row.fullPath)<<"\","
           <<"\"kind\":\""<<(row.isDir?"dir":"file")<<"\","
           <<"\"size\":"<<row.size
           <<"}";
    }
    oss<<"]}";
    return oss.str();
}

std::size_t KeeplyAgentWsClient::writeRaw_(const void* data,std::size_t size){
    if(tls_&&tls_->ssl) return writeAllSsl(tls_->ssl,data,size);
    return writeAllFd(sockfd_,data,size);
}

std::size_t KeeplyAgentWsClient::readRaw_(void* data,std::size_t size){
    if(tls_&&tls_->ssl) return readSomeSsl(tls_->ssl,data,size);
    return readSomeFd(sockfd_,data,size);
}

void KeeplyAgentWsClient::ensureConnected_() const{
    if(!connected_||sockfd_<0) throw std::runtime_error("Cliente websocket do agente nao esta conectado.");
}

} // namespace keeply
