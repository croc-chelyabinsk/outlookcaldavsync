﻿// This file is Part of CalDavSynchronizer (http://outlookcaldavsynchronizer.sourceforge.net/)
// Copyright (c) 2015 Gerhard Zehetbauer
// Copyright (c) 2015 Alexander Nimmervoll
// 
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as
// published by the Free Software Foundation, either version 3 of the
// License, or (at your option) any later version.
// 
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Affero General Public License for more details.
// 
// You should have received a copy of the GNU Affero General Public License
// along with this program.  If not, see <http://www.gnu.org/licenses/>.

using System;
using System.IO;
using System.Net;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;
using System.Xml;
using CalDavSynchronizer.Implementation;
using log4net;

namespace CalDavSynchronizer.DataAccess.HttpClientBasedClient
{
    public class WebDavClient : WebDavClientBase, IWebDavClient
    {
        private static readonly ILog s_logger = LogManager.GetLogger(MethodInfo.GetCurrentMethod().DeclaringType);

        private readonly ProductInfoHeaderValue _productInfo;
        private readonly Func<Task<HttpClient>> _httpClientFactory;
        private HttpClient _httpClient;
        private readonly bool _closeConnectionAfterEachRequest;
        private readonly bool _sendEtagsWithoutQuote;

        public WebDavClient(
            Func<Task<HttpClient>> httpClientFactory,
            string productName,
            string productVersion,
            bool closeConnectionAfterEachRequest,
            bool acceptInvalidChars,
            bool sendEtagsWithoutQuote)
            : base(acceptInvalidChars)
        {
            if (httpClientFactory == null)
                throw new ArgumentNullException("httpClientFactory");

            _productInfo = new ProductInfoHeaderValue(productName, productVersion);
            _httpClientFactory = httpClientFactory;
            _closeConnectionAfterEachRequest = closeConnectionAfterEachRequest;
            _sendEtagsWithoutQuote = sendEtagsWithoutQuote;
        }

        public async Task<XmlDocumentWithNamespaceManager> ExecuteWebDavRequestAndReadResponse(
            Uri url,
            string httpMethod,
            int? depth,
            string ifMatch,
            string ifNoneMatch,
            string mediaType,
            string requestBody)
        {
            try
            {
                var response = await ExecuteWebDavRequest(url, httpMethod, depth, ifMatch, ifNoneMatch, mediaType, requestBody);
                using (response.Item2)
                {
                    using (var responseStream = await response.Item2.Content.ReadAsStreamAsync())
                    {
                        return CreateXmlDocument(responseStream, response.Item3);
                    }
                }
            }
            catch (HttpRequestException x)
            {
                throw WebExceptionMapper.Map(x);
            }
        }

        public async Task<IHttpHeaders> ExecuteWebDavRequestAndReturnResponseHeaders(
            Uri url,
            string httpMethod,
            int? depth,
            string ifMatch,
            string ifNoneMatch,
            string mediaType,
            string requestBody)
        {
            try
            {
                var result = await ExecuteWebDavRequest(url, httpMethod, depth, ifMatch, ifNoneMatch, mediaType, requestBody);
                using (var response = result.Item2)
                {
                    return new HttpResponseHeadersAdapter(result.Item1, response.Headers);
                }
            }
            catch (HttpRequestException x)
            {
                throw WebExceptionMapper.Map(x);
            }
        }

        private async Task<Tuple<HttpResponseHeaders, HttpResponseMessage, Uri>> ExecuteWebDavRequest(
            Uri url,
            string httpMethod,
            int? depth,
            string ifMatch,
            string ifNoneMatch,
            string mediaType,
            string requestBody,
            HttpResponseHeaders headersFromFirstCall = null)
        {
            HttpResponseMessage response;

            using (var requestMessage = new HttpRequestMessage())
            {
                requestMessage.RequestUri = url;
                requestMessage.Headers.UserAgent.Add(_productInfo);
                requestMessage.Method = new HttpMethod(httpMethod);

                if (depth != null)
                    requestMessage.Headers.Add("Depth", depth.ToString());

                if (!string.IsNullOrEmpty(ifMatch))
                {
                    if (_sendEtagsWithoutQuote)
                        requestMessage.Headers.TryAddWithoutValidation("If-Match", ifMatch.Trim('\"'));
                    else
                        requestMessage.Headers.Add("If-Match", ifMatch);
                }

                if (!string.IsNullOrEmpty(ifNoneMatch))
                    requestMessage.Headers.Add("If-None-Match", ifNoneMatch);

                if (!string.IsNullOrEmpty(requestBody))
                {
                    requestMessage.Content = new StringContent(requestBody, Encoding.UTF8, mediaType);
                }

                if (_httpClient == null)
                {
                    _httpClient = await _httpClientFactory();
                    if (_closeConnectionAfterEachRequest)
                        _httpClient.DefaultRequestHeaders.Add("Connection", "close");
                }

                response = await _httpClient.SendAsync(requestMessage);
            }

            try
            {
                if (response.StatusCode == HttpStatusCode.Moved || response.StatusCode == HttpStatusCode.Redirect || response.StatusCode == HttpStatusCode.TemporaryRedirect || response.StatusCode == HttpStatusCode.SeeOther)
                {
                    if (response.Headers.Location != null)
                    {
                        var location = response.Headers.Location;
                        response.Dispose();
                        var effectiveLocation = location.IsAbsoluteUri ? location : new Uri(url, location);

                        //Protocol switched http to https for HSTS
                        if (response.Headers.Contains("Strict-Transport-Security") && effectiveLocation.Scheme == "http")
                        {
                            var newRequestUri = new UriBuilder(effectiveLocation);

                            newRequestUri.Scheme = "https";

                            newRequestUri.Port = url.Port;
                            effectiveLocation = newRequestUri.Uri;
                        }

                        return await ExecuteWebDavRequest(effectiveLocation, httpMethod, depth, ifMatch, ifNoneMatch, mediaType, requestBody, headersFromFirstCall ?? response.Headers);
                    }
                    else
                    {
                        s_logger.Warn("Ignoring Redirection without Location header.");
                    }
                }

                await EnsureSuccessStatusCode(response);

                return Tuple.Create(headersFromFirstCall ?? response.Headers, response, url);
            }
            catch (Exception)
            {
                if (response != null)
                    response.Dispose();
                throw;
            }
        }

        private static async Task EnsureSuccessStatusCode(HttpResponseMessage response)
        {
            if (!response.IsSuccessStatusCode)
            {
                string responseMessage = null;

                try
                {
                    using (var responseStream = await response.Content.ReadAsStreamAsync())
                    {
                        using (var reader = new StreamReader(responseStream, Encoding.UTF8))
                        {
                            responseMessage = reader.ReadToEnd();
                        }
                    }
                }
                catch (Exception x)
                {
                    s_logger.Error("Exception while trying to read the error message.", x);
                }

                throw WebExceptionMapper.Map(
                    new WebDavClientException(
                        response.StatusCode,
                        response.ReasonPhrase ?? response.StatusCode.ToString(),
                        responseMessage,
                        response.Headers != null ? (IHttpHeaders) new HttpResponseHeadersAdapter(response.Headers, response.Headers) : new NullHeaders()));
            }
        }
    }
}