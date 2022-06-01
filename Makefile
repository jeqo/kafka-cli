all:

JAVA_HOME := $(GRAALVM_HOME)
export

native-all:
	./mvnw clean package -Pnative

native-%:
	cd ./$(*) && \
	../mvnw clean package -Pnative