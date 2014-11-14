Name:       ceilometer-publisher-vaultaire
Version:    0.1.0
Release:    0.0anchor%{?build_number}%{!?build_number:1}%{?dist}
Summary:    Vaultaire Publisher for Ceilometer

Group:      Development/Libraries
License:    BSD
URL:        https://github.com/anchor/ceilometer-publisher-vaultaire
Source0:    ceilometer-publisher-vaultaire-%{version}.tar.gz
BuildRoot:  %(mktemp -ud %{_tmppath}/%{name}-%{version}-%{release}-XXXXXX)

BuildRequires:  ghc >= 7.8.3
BuildRequires:  cabal-install
BuildRequires:  zeromq-devel >= 4.0.3
BuildRequires:  gmp-devel
Requires:   gmp
Requires:   zeromq >= 4.0.3

%description
ceilometer-publisher-vaultaire reads metrics from a RabbitMQ queue, consolidates them and publishes them as vaultaire SimplePoints and SourceDicts

%global ghc_without_dynamic 1

%prep
%setup

%build

%install
mkdir -p %{buildroot}%{_bindir}
install -m 0755 check-page-fragmentation %{buildroot}%{_bindir}

%clean
rm -rf %{buildroot}

%post
echo "Nagios checks installed:"
echo "    /usr/bin/check-page-fragmentation"

%files
%defattr(-,root,root,-)
%{_bindir}/check-page-fragmentation

%changelog
* Thu Nov 13 2014 Oswyn Brent <oswyn.brent@anchor.com.au> - 0.1.0-0.0anchor1
- initial build

